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

	"github.com/openGemini/openGemini/lib/record"
)

// unorderedHeapSource is one out-of-order segment record acting as a heap source. priority is
// the read order of the segment (the outOfOrderLocations cursor yields segments depth-first per
// location, in seq-ascending location order, so a higher priority segment was read later and is
// newer). At a duplicate timestamp the higher-priority (newer) source's non-nil column wins,
// matching the chain MergeRecord's "last-processed (highest seq) = newRec = wins" semantics.
type unorderedHeapSource struct {
	rec      *record.Record
	pos      int
	priority int
}

func (s *unorderedHeapSource) curTime() int64 {
	return s.rec.Times()[s.pos]
}

// unorderedHeap is a heap of sources by current row time, tie-broken by priority descending so
// that within a same-timestamp group the newest source pops first. For ascending it is a min-heap
// (smallest time first); for descending a max-heap (largest time first).
type unorderedHeap struct {
	items     []*unorderedHeapSource
	ascending bool
}

func (h unorderedHeap) Len() int { return len(h.items) }
func (h unorderedHeap) Less(i, j int) bool {
	ti := h.items[i].curTime()
	tj := h.items[j].curTime()
	if ti != tj {
		if h.ascending {
			return ti < tj
		}
		return ti > tj
	}
	return h.items[i].priority > h.items[j].priority
}
func (h *unorderedHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *unorderedHeap) Push(x any)    { h.items = append(h.items, x.(*unorderedHeapSource)) }
func (h *unorderedHeap) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[:n-1]
	return x
}

// heapMergeUnordered K-way merges a set of out-of-order segment records into a single
// time-sorted, deduplicated record, producing the same outRec as the chain MergeRecord loop but
// in O(M*logK) instead of O(N^2*R): each output row is produced once (one heap pop) rather than
// being rescanned at every subsequent chain merge.
//
// All input records share ctx.schema (the reader decodes into a ctx.schema-built dst and pads
// missing fields with nils via TryPadColumn), so columns align by index. Same-timestamp rows are
// folded by priority (newest non-nil wins), equivalent to mergeRecRow folded newest-to-oldest.
//
// Each segment is one live heap source, so same-timestamp rows across segments (whether the same
// file or different files) are naturally grouped and folded — no cross-segment catch-up is
// needed. Duplicate timestamps within a single segment are emitted as multiple rows, matching the
// chain merge which keeps intra-record duplicates (appendRecs appends each) while folding
// cross-record duplicates.
func heapMergeUnordered(sources []*unorderedHeapSource, schema record.Schemas, ascending bool, isAborted func() bool) *record.Record {
	out := record.NewRecordBuilder(schema)
	h := &unorderedHeap{ascending: ascending}
	numCols := len(schema)
	// Size the output columns to the total input size up front. Appending one row at a time would
	// grow each column's backing slice incrementally; Go grows large slices by ~1.25x (not 2x), so
	// building a multi-GB string column that way allocates ~6x its final size. Reserving once
	// allocates ~1x and avoids the growth entirely. Output may be smaller than the input when
	// same-timestamp rows fold, in which case the reserved capacity is simply unused.
	totalRows := 0
	totalValBytes := make([]int, numCols)
	for _, s := range sources {
		if s.rec == nil || s.rec.RowNums() == 0 {
			continue
		}
		s.pos = 0
		heap.Push(h, s)
		totalRows += s.rec.RowNums()
		for c := 0; c < numCols; c++ {
			totalValBytes[c] += len(s.rec.ColVals[c].Val)
		}
	}
	if totalRows > 0 {
		for c := 0; c < numCols; c++ {
			cv := &out.ColVals[c]
			cv.Val = make([]byte, 0, totalValBytes[c])
			cv.Offset = make([]uint32, 0, totalRows)
			cv.Bitmap = make([]byte, 0, totalRows/8+1)
		}
	}

	var group []*unorderedHeapSource
	for h.Len() > 0 {
		if isAborted != nil && isAborted() {
			// Discard any partial batch so aborted rows are never returned to the caller.
			return nil
		}
		t := h.items[0].curTime()
		group = group[:0]
		for h.Len() > 0 && h.items[0].curTime() == t {
			s := heap.Pop(h).(*unorderedHeapSource)
			group = append(group, s)
		}
		appendMergedSameTimeRow(out, group, t)

		for _, s := range group {
			s.pos++
			if s.pos >= s.rec.RowNums() {
				s.rec = nil // release the segment record for GC once fully consumed
			} else {
				heap.Push(h, s)
			}
		}
	}
	if out.RowNums() == 0 {
		return nil
	}
	return out
}

// appendMergedSameTimeRow appends one merged row to out for a same-timestamp group (sources whose
// current row is t, ordered newest-first by priority). For each non-time column the newest
// source's non-nil value wins; if every source is nil at that column, a nil is appended. The time
// column is appended once.
func appendMergedSameTimeRow(out *record.Record, group []*unorderedHeapSource, t int64) {
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

// chainMergeUnorderedRecords replicates the original FirstTimeInit chain merge over a slice of
// segment records: each record is merged into the accumulator as the newRec (so the last record
// wins at a duplicate timestamp). recs must be in priority-ascending order (read order). It is
// retained as the byte-identical fallback for the case heapMergeUnordered cannot replicate exactly
// (intra-segment duplicate timestamps — see hasIntraSegmentDupTimes).
func chainMergeUnorderedRecords(recs []*record.Record, ascending bool) *record.Record {
	if len(recs) == 0 {
		return nil
	}
	outRec := recs[0]
	for i := 1; i < len(recs); i++ {
		var merged record.Record
		if ascending {
			merged.MergeRecord(recs[i], outRec)
		} else {
			merged.MergeRecordDescend(recs[i], outRec)
		}
		outRec = &merged
	}
	return outRec
}

// hasIntraSegmentDupTimes reports whether any segment record contains consecutive equal
// timestamps (segments are time-sorted, so duplicates are adjacent). When true, the chain merge
// fallback must be used: MergeRecord's overlap region is computed by GetTimeRangeStartIndex, whose
// binary search lands on an arbitrary duplicate, so the heap's deterministic same-time grouping
// cannot match it byte-for-byte when an intra-segment duplicate also appears across segments.
// Absent intra-segment duplicates the two algorithms produce identical output.
func hasIntraSegmentDupTimes(recs []*record.Record) bool {
	for _, r := range recs {
		if r == nil {
			continue
		}
		times := r.Times()
		for i := 1; i < len(times); i++ {
			if times[i] == times[i-1] {
				return true
			}
		}
	}
	return false
}
