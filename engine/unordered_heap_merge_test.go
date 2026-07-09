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
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var heapMergeSchema = record.Schemas{
	{Type: influx.Field_Type_Int, Name: "value"},
	{Type: influx.Field_Type_Int, Name: record.TimeField},
}

type mergeRow struct {
	t    int64
	v    int64
	nilV bool
}

func buildMergeRec(rows []mergeRow) *record.Record {
	rec := record.NewRecordBuilder(heapMergeSchema)
	for _, r := range rows {
		if r.nilV {
			rec.ColVals[0].AppendIntegerNull()
		} else {
			rec.ColVals[0].AppendInteger(r.v)
		}
		rec.AppendTime(r.t)
	}
	return rec
}

func flattenRec(rec *record.Record) []mergeRow {
	if rec == nil || rec.RowNums() == 0 {
		return nil
	}
	n := rec.RowNums()
	out := make([]mergeRow, n)
	times := rec.Times()
	for i := 0; i < n; i++ {
		v, isNil := rec.ColVals[0].IntegerValue(i)
		out[i] = mergeRow{t: times[i], v: v, nilV: isNil}
	}
	return out
}

func heapMergeFromRecs(recs []*record.Record, ascending bool) *record.Record {
	sources := make([]*unorderedHeapSource, len(recs))
	for i, r := range recs {
		sources[i] = &unorderedHeapSource{rec: r, priority: i}
	}
	return heapMergeUnordered(sources, heapMergeSchema, ascending, nil)
}

// mergeOutUnorderedRoutes mirrors FirstTimeInit's routing: heap when no intra-segment duplicate
// timestamps, chain merge (byte-identical to the original eager path) otherwise.
func mergeOutUnorderedRoutes(recs []*record.Record, ascending bool) *record.Record {
	if hasIntraSegmentDupTimes(recs) {
		return chainMergeUnorderedRecords(recs, ascending)
	}
	return heapMergeFromRecs(recs, ascending)
}

// distinctRows builds nRows rows with distinct timestamps drawn from [0, timeRange), so no
// intra-segment duplicate timestamps exist (cross-segment duplicates are still possible and
// expected). Rows are returned time-sorted for the requested direction.
func distinctRows(rng *rand.Rand, nRows, timeRange int, ascending bool) []mergeRow {
	if nRows > timeRange {
		nRows = timeRange
	}
	perm := rng.Perm(timeRange)
	rows := make([]mergeRow, nRows)
	for i := 0; i < nRows; i++ {
		rows[i] = mergeRow{t: int64(perm[i]), v: int64(rng.Intn(100)), nilV: rng.Intn(4) == 0}
	}
	sort.SliceStable(rows, func(a, b int) bool {
		if ascending {
			return rows[a].t < rows[b].t
		}
		return rows[a].t > rows[b].t
	})
	return rows
}

// TestHeapMergeUnorderedDifferential compares the heap against the chain-merge oracle over random
// inputs with no intra-segment duplicate timestamps (the case the heap is used for). Under that
// precondition the two algorithms produce byte-identical output.
func TestHeapMergeUnorderedDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for trial := 0; trial < 20000; trial++ {
		ascending := rng.Intn(2) == 0
		nSeg := 1 + rng.Intn(5)
		recs := make([]*record.Record, nSeg)
		for i := 0; i < nSeg; i++ {
			recs[i] = buildMergeRec(distinctRows(rng, 1+rng.Intn(6), 8, ascending))
		}
		chainOut := flattenRec(chainMergeUnorderedRecords(recs, ascending))
		heapOut := flattenRec(heapMergeFromRecs(recs, ascending))
		if !reflect.DeepEqual(chainOut, heapOut) {
			t.Fatalf("trial %d (%v) mismatch:\n recs=%v\n chain=%v\n heap =%v",
				trial, ascending, recsDebug(recs), chainOut, heapOut)
		}
	}
}

// TestMergeRoutesChainOnIntraDup verifies the routing: when an intra-segment duplicate timestamp
// exists, the routed output equals the chain merge (byte-identical to the original eager path),
// and may differ from the raw heap.
func TestMergeRoutesChainOnIntraDup(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for trial := 0; trial < 5000; trial++ {
		ascending := rng.Intn(2) == 0
		nSeg := 1 + rng.Intn(4)
		recs := make([]*record.Record, nSeg)
		for i := 0; i < nSeg; i++ {
			// allow intra-segment duplicate timestamps (rng.Intn(6) over range 4)
			nRows := 1 + rng.Intn(6)
			rows := make([]mergeRow, nRows)
			for j := range rows {
				rows[j] = mergeRow{t: int64(rng.Intn(4)), v: int64(rng.Intn(100)), nilV: rng.Intn(4) == 0}
			}
			sort.SliceStable(rows, func(a, b int) bool {
				if ascending {
					return rows[a].t < rows[b].t
				}
				return rows[a].t > rows[b].t
			})
			recs[i] = buildMergeRec(rows)
		}
		routed := flattenRec(mergeOutUnorderedRoutes(recs, ascending))
		chain := flattenRec(chainMergeUnorderedRecords(recs, ascending))
		if !reflect.DeepEqual(routed, chain) {
			t.Fatalf("trial %d routed != chain:\n recs=%v\n routed=%v\n chain=%v",
				trial, recsDebug(recs), routed, chain)
		}
	}
}

func recsDebug(recs []*record.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = ""
		if r != nil {
			out[i] = r.String()
		}
	}
	return out
}

// TestHeapMergeCrossSegmentFold covers the core correctness case: the same timestamp across
// segments is folded to one row with the newest-priority value winning.
func TestHeapMergeCrossSegmentFold(t *testing.T) {
	cases := []struct {
		name      string
		ascending bool
		recs      [][]mergeRow
	}{
		{
			name:      "asc_fold_newer_wins",
			ascending: true,
			recs: [][]mergeRow{
				{{t: 5, v: 10}, {t: 7, v: 70}},
				{{t: 5, v: 99}, {t: 6, v: 60}},
			},
		},
		{
			name:      "asc_nil_filled_by_newer",
			ascending: true,
			recs: [][]mergeRow{
				{{t: 5, v: 0, nilV: true}},
				{{t: 5, v: 42}},
			},
		},
		{
			name:      "desc_fold_newer_wins",
			ascending: false,
			recs: [][]mergeRow{
				{{t: 7, v: 70}, {t: 5, v: 10}},
				{{t: 6, v: 60}, {t: 5, v: 99}},
			},
		},
	}
	for _, tc := range cases {
		recs := make([]*record.Record, len(tc.recs))
		for i, rows := range tc.recs {
			recs[i] = buildMergeRec(rows)
		}
		chainOut := flattenRec(chainMergeUnorderedRecords(recs, tc.ascending))
		heapOut := flattenRec(heapMergeFromRecs(recs, tc.ascending))
		if !reflect.DeepEqual(chainOut, heapOut) {
			t.Errorf("case %q mismatch\n chain=%v\n heap =%v", tc.name, chainOut, heapOut)
		}
		if tc.name == "asc_fold_newer_wins" {
			if len(heapOut) != 3 || heapOut[0].t != 5 || heapOut[0].v != 99 {
				t.Errorf("case %q unexpected output %v", tc.name, heapOut)
			}
		}
	}
}

func TestHasIntraSegmentDupTimes(t *testing.T) {
	if hasIntraSegmentDupTimes(nil) {
		t.Error("nil recs should be false")
	}
	noDup := []*record.Record{buildMergeRec([]mergeRow{{t: 1, v: 1}, {t: 2, v: 2}})}
	if hasIntraSegmentDupTimes(noDup) {
		t.Error("distinct times should be false")
	}
	dup := []*record.Record{buildMergeRec([]mergeRow{{t: 1, v: 1}, {t: 1, v: 2}, {t: 3, v: 3}})}
	if !hasIntraSegmentDupTimes(dup) {
		t.Error("consecutive equal times should be detected")
	}
}

func TestHeapMergeEmpty(t *testing.T) {
	if got := heapMergeFromRecs(nil, true); got != nil {
		t.Errorf("nil recs = %v, want nil", got)
	}
	if got := heapMergeFromRecs([]*record.Record{buildMergeRec(nil)}, true); got != nil {
		t.Errorf("empty rec = %v, want nil", got)
	}
}

// --- multi-field (int + float) differential: per-column fold, independent nils, type dispatch ---

var heapMergeSchema2 = record.Schemas{
	{Type: influx.Field_Type_Int, Name: "a"},
	{Type: influx.Field_Type_Float, Name: "f"},
	{Type: influx.Field_Type_Int, Name: record.TimeField},
}

type mergeRow2 struct {
	t    int64
	a    int64
	aNil bool
	f    float64
	fNil bool
}

func buildMergeRec2(rows []mergeRow2) *record.Record {
	rec := record.NewRecordBuilder(heapMergeSchema2)
	for _, r := range rows {
		if r.aNil {
			rec.ColVals[0].AppendIntegerNull()
		} else {
			rec.ColVals[0].AppendInteger(r.a)
		}
		if r.fNil {
			rec.ColVals[1].AppendFloatNull()
		} else {
			rec.ColVals[1].AppendFloat(r.f)
		}
		rec.AppendTime(r.t)
	}
	return rec
}

func flattenRec2(rec *record.Record) []mergeRow2 {
	if rec == nil || rec.RowNums() == 0 {
		return nil
	}
	n := rec.RowNums()
	out := make([]mergeRow2, n)
	times := rec.Times()
	for i := 0; i < n; i++ {
		a, aNil := rec.ColVals[0].IntegerValue(i)
		f, fNil := rec.ColVals[1].FloatValue(i)
		out[i] = mergeRow2{t: times[i], a: a, aNil: aNil, f: f, fNil: fNil}
	}
	return out
}

func distinctRows2(rng *rand.Rand, nRows, timeRange int, ascending bool) []mergeRow2 {
	if nRows > timeRange {
		nRows = timeRange
	}
	perm := rng.Perm(timeRange)
	rows := make([]mergeRow2, nRows)
	for i := 0; i < nRows; i++ {
		rows[i] = mergeRow2{
			t: int64(perm[i]), a: int64(rng.Intn(100)), aNil: rng.Intn(3) == 0,
			f: float64(rng.Intn(1000)) / 10, fNil: rng.Intn(3) == 0,
		}
	}
	sort.SliceStable(rows, func(a, b int) bool {
		if ascending {
			return rows[a].t < rows[b].t
		}
		return rows[a].t > rows[b].t
	})
	return rows
}

func TestHeapMergeMultiFieldDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	for trial := 0; trial < 10000; trial++ {
		ascending := rng.Intn(2) == 0
		nSeg := 1 + rng.Intn(5)
		recs := make([]*record.Record, nSeg)
		for i := 0; i < nSeg; i++ {
			recs[i] = buildMergeRec2(distinctRows2(rng, 1+rng.Intn(6), 8, ascending))
		}
		chainOut := flattenRec2(chainMergeUnorderedRecords(recs, ascending))
		sources := make([]*unorderedHeapSource, len(recs))
		for i, r := range recs {
			sources[i] = &unorderedHeapSource{rec: r, priority: i}
		}
		heapOut := flattenRec2(heapMergeUnordered(sources, heapMergeSchema2, ascending, nil))
		if !reflect.DeepEqual(chainOut, heapOut) {
			t.Fatalf("multi-field trial %d (%v) mismatch\n chain=%v\n heap =%v",
				trial, ascending, chainOut, heapOut)
		}
	}
}

// TestHeapMergeNewerNilFilledByOlder: at a shared timestamp, the newer (higher priority) source's
// non-nil value wins per column; a nil in the newer source is filled by the older source.
func TestHeapMergeNewerNilFilledByOlder(t *testing.T) {
	// rec0 (priority 0, older): a=10, f=nil. rec1 (priority 1, newer): a=nil, f=5.5.
	// At t=5 the merged row should be a=10 (from older, newer nil), f=5.5 (from newer).
	rec0 := buildMergeRec2([]mergeRow2{{t: 5, a: 10, aNil: false, f: 0, fNil: true}})
	rec1 := buildMergeRec2([]mergeRow2{{t: 5, a: 0, aNil: true, f: 5.5, fNil: false}})
	recs := []*record.Record{rec0, rec1}
	chainOut := flattenRec2(chainMergeUnorderedRecords(recs, true))
	heapOut := flattenRec2(heapMergeFromRecs2(recs, true))
	if !reflect.DeepEqual(chainOut, heapOut) {
		t.Fatalf("mismatch\n chain=%v\n heap =%v", chainOut, heapOut)
	}
	if len(heapOut) != 1 || heapOut[0].a != 10 || heapOut[0].f != 5.5 {
		t.Errorf("unexpected merge %v", heapOut)
	}
}

func heapMergeFromRecs2(recs []*record.Record, ascending bool) *record.Record {
	sources := make([]*unorderedHeapSource, len(recs))
	for i, r := range recs {
		sources[i] = &unorderedHeapSource{rec: r, priority: i}
	}
	return heapMergeUnordered(sources, heapMergeSchema2, ascending, nil)
}

// TestHeapMergeAbort: an aborted merge discards any partial batch and returns nil.
func TestHeapMergeAbort(t *testing.T) {
	recs := []*record.Record{
		buildMergeRec([]mergeRow{{t: 1, v: 1}, {t: 2, v: 2}, {t: 3, v: 3}}),
		buildMergeRec([]mergeRow{{t: 1, v: 9}, {t: 2, v: 8}, {t: 3, v: 7}}),
	}
	sources := make([]*unorderedHeapSource, len(recs))
	for i, r := range recs {
		sources[i] = &unorderedHeapSource{rec: r, priority: i}
	}
	// Abort immediately.
	if got := heapMergeUnordered(sources, heapMergeSchema, true, func() bool { return true }); got != nil {
		t.Errorf("aborted merge = %v, want nil", got)
	}
	// Abort after the first row is produced: still must return nil (partial discarded).
	count := 0
	sources2 := make([]*unorderedHeapSource, len(recs))
	for i, r := range recs {
		sources2[i] = &unorderedHeapSource{rec: r, priority: i}
	}
	if got := heapMergeUnordered(sources2, heapMergeSchema, true, func() bool {
		count++
		return count > 1
	}); got != nil {
		t.Errorf("mid-merge abort = %v, want nil (partial discarded)", got)
	}
}
