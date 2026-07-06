// Copyright 2024 openGemini Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"sort"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/record"
)

// lazyUnorderedMerger K-way merges the records read from a set of out-of-order locations into a
// single time-sorted, deduplicated stream, producing batches bounded by an ordered watermark.
//
// It replaces the eager chain merge in FirstTimeInit (which reads every matched out-of-order
// location up front and folds them with repeated MergeRecord calls, O((K*B)^2) worst case) with
// an O(rows*log-or-K) merge that only reads segments at or before the current watermark.
//
// Same-timestamp precedence matches the eager path: out-of-order files are sorted by sequence,
// and at a duplicate timestamp the higher-sequence (newer) record's non-nil column wins, with
// older records filling nil columns. This is equivalent to mergeRecRow folded over the
// same-time group from newest to oldest. All input records share ctx.schema, so column indices
// align directly.
//
// Only ascending non-aggregate reads are supported; other query shapes fall back to the eager
// path. Correctness is validated by differential testing against the eager path.
type lazyUnorderedMerger struct {
	schema     record.Schemas
	filterOpts *immutable.FilterOptions
	sources    []*unorderedSource
}

type unorderedSource struct {
	loc  *immutable.Location
	seq  uint64
	rec  *record.Record
	pos  int
	done bool
}

func newLazyUnorderedMerger(schema record.Schemas, filterOpts *immutable.FilterOptions, locations *immutable.LocationCursor) *lazyUnorderedMerger {
	m := &lazyUnorderedMerger{schema: schema, filterOpts: filterOpts}
	for i := 0; i < locations.Len(); i++ {
		loc := locations.LocationAt(i)
		_, seq := loc.Sequence()
		m.sources = append(m.sources, &unorderedSource{loc: loc, seq: seq})
	}
	return m
}

// readNext reads the next segment of the source whose time range is at or before watermark.
// Returns a non-nil record when a qualifying segment is read, nil when the source is deferred
// (next segment is beyond the watermark) or exhausted.
func (s *unorderedSource) readNext(filterOpts *immutable.FilterOptions, schema record.Schemas, watermark int64) (*record.Record, error) {
	if !s.loc.HasNext() {
		s.done = true
		return nil, nil
	}
	dst := record.NewRecordBuilder(schema)
	return s.loc.ReadDataBeforeWatermark(filterOpts, dst, watermark)
}

func (s *unorderedSource) curTime() int64 {
	return s.rec.Times()[s.pos]
}

// nextBatchUntil merges ALL rows with time <= watermark into a single record. Every row at or
// before the watermark must be admitted together so that mergeData can merge them with the
// ordered batch without missing a same-timestamp out-of-order row. Rows beyond the watermark
// remain buffered in their sources for a later batch. Returns nil when no rows at or before
// the watermark are available.
func (m *lazyUnorderedMerger) nextBatchUntil(watermark int64) (*record.Record, error) {
	// No row cap: emit every row <= watermark. The caller (mergeData) splits the result into
	// maxRowCnt-sized outputs.
	return m.nextBatch(&watermark, 1<<30)
}

// nextBatch merges up to maxRows rows. If watermark is non-nil, only rows with time <= watermark
// are emitted; if nil, all available rows are emitted (unordered-only fallback path).
func (m *lazyUnorderedMerger) nextBatch(watermark *int64, maxRows int) (*record.Record, error) {
	out := record.NewRecordBuilder(m.schema)
	for out.RowNums() < maxRows {
		// Admit/refill: read the next qualifying segment for every source that needs one.
		for _, s := range m.sources {
			if s.rec != nil || s.done {
				continue
			}
			rec, err := s.readNext(m.filterOpts, m.schema, m.watermarkOrMax(watermark))
			if err != nil {
				return nil, err
			}
			if rec != nil {
				s.rec = rec
				s.pos = 0
			} else if !s.loc.HasNext() {
				s.done = true
			}
		}

		// Find the minimum current time across live sources.
		var minT int64
		hasAny := false
		for _, s := range m.sources {
			if s.rec == nil {
				continue
			}
			t := s.curTime()
			if !hasAny || t < minT {
				minT = t
				hasAny = true
			}
		}
		if !hasAny {
			break // all sources done or deferred
		}
		if watermark != nil && minT > *watermark {
			break // remaining rows are beyond the watermark; defer
		}

		// Collect the same-time group (all live sources whose current row == minT), newest
		// sequence first so column-level nil fill matches mergeRecRow folded newest-to-oldest.
		group := make([]*unorderedSource, 0)
		for _, s := range m.sources {
			if s.rec != nil && s.curTime() == minT {
				group = append(group, s)
			}
		}
		sort.Slice(group, func(i, j int) bool { return group[i].seq > group[j].seq })
		appendMergedSameTimeRow(out, group, minT)

		// Advance each grouped source; exhausted records are refilled on the next loop pass.
		for _, s := range group {
			s.pos++
			if s.pos >= s.rec.RowNums() {
				s.rec = nil
			}
		}
	}
	if out.RowNums() == 0 {
		return nil, nil
	}
	return out, nil
}

func (m *lazyUnorderedMerger) watermarkOrMax(watermark *int64) int64 {
	if watermark == nil {
		// unordered-only path: admit every remaining segment regardless of time.
		return 1<<63 - 1
	}
	return *watermark
}

// allDone reports whether every source is exhausted (no more data to admit).
func (m *lazyUnorderedMerger) allDone() bool {
	for _, s := range m.sources {
		if s.rec != nil || !s.done {
			return false
		}
	}
	return true
}

// appendMergedSameTimeRow appends one merged row to out for a same-timestamp group. For each
// non-time column, the newest (highest-sequence) source's non-nil value wins; if every source
// is nil at that column, a nil is appended. The time column is appended once.
func appendMergedSameTimeRow(out *record.Record, group []*unorderedSource, t int64) {
	numCols := len(out.Schema) - 1 // exclude the trailing time column
	for c := 0; c < numCols; c++ {
		appended := false
		for _, s := range group {
			if !s.rec.ColVals[c].IsNil(s.pos) {
				out.ColVals[c].AppendColVal(&s.rec.ColVals[c], out.Schema[c].Type, s.pos, s.pos+1)
				appended = true
				break
			}
		}
		if !appended {
			out.ColVals[c].PadColVal(out.Schema[c].Type, 1)
		}
	}
	out.AppendTime(t)
}
