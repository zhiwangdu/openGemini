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
	"container/heap"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/record"
)

// lazyUnorderedMerger K-way merges the records read from a set of out-of-order locations into a
// single time-sorted, deduplicated stream, producing maxRowCnt-sized batches.
//
// It replaces the eager chain merge in FirstTimeInit (which reads every matched out-of-order
// location up front and folds them with repeated MergeRecord calls, O(N^2 * R) for N files of R
// rows) with a heap K-way merge that is O(M * log K) in the row count M = N*R, while keeping
// only the current segment from each source live.
//
// Same-timestamp precedence matches the eager path: out-of-order files are sorted by sequence,
// and at a duplicate timestamp the higher-sequence (newer) record's non-nil column wins, with
// older records filling nil columns. This is equivalent to mergeRecRow folded over the
// same-time group from newest to oldest. All input records share ctx.schema, so column indices
// align directly.
//
// Non-aggregate reads (ascending and descending) are supported; other query shapes fall back
// to the eager path. Correctness is validated by differential testing against the eager path.
type lazyUnorderedMerger struct {
	schema     record.Schemas
	filterOpts *immutable.FilterOptions
	sources    []*unorderedSource
	heap       lazyMergerHeap
	isAborted  func() bool
	ascending  bool
	group      []*unorderedSource // reused scratch for same-time groups
}

type unorderedSource struct {
	loc    *immutable.Location
	seq    uint64
	rec    *record.Record
	pos    int
	done   bool
	inHeap bool
}

// lazyMergerHeap is a heap of sources by current row time, tie-broken by sequence descending
// so that within a same-timestamp group the newest (highest-sequence) source is popped first.
// For ascending it is a min-heap (smallest time pops first); for descending a max-heap
// (largest time pops first).
type lazyMergerHeap struct {
	items     []*unorderedSource
	ascending bool
}

func (h lazyMergerHeap) Len() int { return len(h.items) }
func (h lazyMergerHeap) Less(i, j int) bool {
	ti := h.items[i].curTime()
	tj := h.items[j].curTime()
	if ti != tj {
		if h.ascending {
			return ti < tj
		}
		return ti > tj
	}
	return h.items[i].seq > h.items[j].seq
}
func (h *lazyMergerHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *lazyMergerHeap) Push(x any)    { h.items = append(h.items, x.(*unorderedSource)) }
func (h *lazyMergerHeap) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[:n-1]
	return x
}

func newLazyUnorderedMerger(schema record.Schemas, filterOpts *immutable.FilterOptions, locations *immutable.LocationCursor, isAborted func() bool, ascending bool) *lazyUnorderedMerger {
	m := &lazyUnorderedMerger{
		schema:     schema,
		filterOpts: filterOpts,
		isAborted:  isAborted,
		ascending:  ascending,
		heap:       lazyMergerHeap{ascending: ascending},
	}
	for i := 0; i < locations.Len(); i++ {
		loc := locations.LocationAt(i)
		_, seq := loc.Sequence()
		m.sources = append(m.sources, &unorderedSource{loc: loc, seq: seq})
	}
	return m
}

// readNext reads the next qualifying segment of the source. Returns nil when the source is
// exhausted or the query was aborted.
func (s *unorderedSource) readNext(filterOpts *immutable.FilterOptions, schema record.Schemas) (*record.Record, error) {
	if !s.loc.HasNext() {
		s.done = true
		return nil, nil
	}
	dst := record.NewRecordBuilder(schema)
	return s.loc.ReadData(filterOpts, dst, nil)
}

func (s *unorderedSource) curTime() int64 {
	return s.rec.Times()[s.pos]
}

// nextBatch merges up to maxRows rows from the unordered sources.
func (m *lazyUnorderedMerger) nextBatch(maxRows int) (*record.Record, error) {
	out := record.NewRecordBuilder(m.schema)

	// Admit once per call: read the next qualifying segment for every source that is idle (not in
	// the heap, not done). This is O(K) per call, not per row; per-row work below is O(log K) via
	// the heap.
	for _, s := range m.sources {
		if s.inHeap || s.done || s.rec != nil {
			continue
		}
		if err := m.admit(s); err != nil {
			return nil, err
		}
	}

	for out.RowNums() < maxRows {
		// If the query is aborted, stop merging and discard any partial batch so aborted rows
		// are never returned to the caller. An aborted cursor is not consumed further and is
		// reset (lazyMerger is nil'd) before reuse.
		if m.isAborted != nil && m.isAborted() {
			return nil, nil
		}
		if m.heap.Len() == 0 {
			break // all sources done or currently have no readable segment
		}
		top := m.heap.items[0]
		t := top.curTime()

		// Pop the same-time group (all heap-top sources whose current row == t). The heap's
		// seq-desc tie-break makes group[0] the newest, matching mergeRecRow folded newest-to-oldest.
		group := m.group[:0]
		for m.heap.Len() > 0 && m.heap.items[0].curTime() == t {
			s := heap.Pop(&m.heap).(*unorderedSource)
			s.inHeap = false
			group = append(group, s)
		}
		appendMergedSameTimeRow(out, group, t)
		m.group = group // keep the scratch slice for reuse

		// Advance each grouped source; exhausted sources are refilled inline (O(log K) per source)
		// so the per-row cost stays logarithmic in K rather than scanning all sources.
		for _, s := range group {
			s.pos++
			if s.pos >= s.rec.RowNums() {
				s.rec = nil
				if err := m.admit(s); err != nil {
					return nil, err
				}
			} else {
				s.inHeap = true
				heap.Push(&m.heap, s)
			}
		}
	}
	if out.RowNums() == 0 {
		return nil, nil
	}
	return out, nil
}

// admit reads the next qualifying segment for a source and pushes it into the heap. If the
// source is exhausted it is marked done.
func (m *lazyUnorderedMerger) admit(s *unorderedSource) error {
	rec, err := s.readNext(m.filterOpts, m.schema)
	if err != nil {
		return err
	}
	if rec != nil {
		s.rec = rec
		s.pos = 0
		s.inHeap = true
		heap.Push(&m.heap, s)
	} else if !s.loc.HasNext() {
		s.done = true
	}
	return nil
}

// allDone reports whether every source is exhausted (no more data to admit).
func (m *lazyUnorderedMerger) allDone() bool {
	for _, s := range m.sources {
		if s.rec != nil || s.inHeap || !s.done {
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
