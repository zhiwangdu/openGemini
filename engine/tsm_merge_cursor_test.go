// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	assert2 "github.com/stretchr/testify/assert"
)

func TestAddLocationsWithLimit(t *testing.T) {
	files := []immutable.TSSPFile{MocTsspFile{}, MocTsspFile{}}
	l := &immutable.LocationCursor{}
	opt := &query.ProcessorOptions{
		Ascending: false,
		Limit:     3,
		StartTime: 0,
		EndTime:   10,
	}
	qs := &executor.QuerySchema{}
	qs.SetOpt(opt)
	ctx := &idKeyCursorContext{
		schema:      record.Schemas{record.Field{Type: influx.Field_Type_Int, Name: "value"}},
		querySchema: qs,
		decs:        immutable.NewReadContext(qs.Options().IsAscending()),
		tr:          util.TimeRange{Min: 0, Max: 10},
	}
	AddLocationsWithLimit(l, files, ctx, 0527)
	assert2.Equal(t, 1, l.Len())
}

func TestNotAddLocationsWithLimit(t *testing.T) {
	files := []immutable.TSSPFile{MocTsspFile{}, MocTsspFile{}}
	l := &immutable.LocationCursor{}
	opt := &query.ProcessorOptions{
		Ascending: false,
		Limit:     3,
		StartTime: 0,
		EndTime:   5,
	}
	qs := &executor.QuerySchema{}
	qs.SetOpt(opt)
	ctx := &idKeyCursorContext{
		schema:      record.Schemas{record.Field{Type: influx.Field_Type_Int, Name: "value"}},
		querySchema: qs,
		decs:        immutable.NewReadContext(qs.Options().IsAscending()),
		tr:          util.TimeRange{Min: 0, Max: 5},
	}
	AddLocationsWithLimit(l, files, ctx, 0527)
	assert2.Equal(t, 2, l.Len())
}

// TestFirstTimeInitEmptyOutOfOrder exercises the path where every matched out-of-order location
// yields a nil record (here the mock ReadAt returns nil), so outRec stays nil. record.RowNums is
// nil-safe, so the span-count path returns 0 rows without error; Next returns nil. Also verifies
// the unordered location/merge counters are recorded.
func TestFirstTimeInitEmptyOutOfOrder(t *testing.T) {
	files := []immutable.TSSPFile{mocTsspFileNilReadAt{MocTsspFile{}}}
	opt := &query.ProcessorOptions{Ascending: true, Limit: 100, StartTime: 0, EndTime: 10}
	qs := &executor.QuerySchema{}
	qs.SetOpt(opt)
	closedSignal := false
	ctx := &idKeyCursorContext{
		schema:       record.Schemas{record.Field{Type: influx.Field_Type_Int, Name: "value"}},
		querySchema:  qs,
		decs:         immutable.NewReadContext(qs.Options().IsAscending()),
		tr:           util.TimeRange{Min: 0, Max: 10},
		tmsMergePool: TsmMergePool,
		maxRowCnt:    100,
		readers:      &immutable.MmsReaders{OutOfOrders: files},
		closedSignal: &closedSignal,
	}

	cursor, err := newTsmMergeCursor(ctx, 0527, nil, nil, nil, false, nil)
	assert2.NoError(t, err)
	assert2.NotNil(t, cursor)

	_, span := tracing.NewTrace("root")
	span.CreateCounter(unorderRowCount, "")
	span.CreateCounter(unorderDuration, "ns")
	span.CreateCounter(tsmIterCount, "")
	cursor.StartSpan(span)

	// FirstTimeInit reads the matched out-of-order location, but the mock ReadAt returns nil,
	// so outRec stays nil. RowNums is nil-safe, so no error; Next returns nil.
	rec, err := cursor.Next()
	assert2.NoError(t, err)
	assert2.Nil(t, rec)

	// Observability: the matched out-of-order location count and merge count are recorded.
	assert2.Equal(t, "1", span.CreateCounter(unorderedLocationCount, "").Value())
	assert2.Equal(t, "0", span.CreateCounter(unorderedMergeCount, "").Value())
	cursor.Close()
}

// mocTsspFileNilReadAt wraps MocTsspFile but reports a single-segment chunk meta (so the
// location is matched and the segment cursor is well-formed) and returns nil from ReadAt,
// simulating a location whose rows are all filtered away.
type mocTsspFileNilReadAt struct {
	MocTsspFile
}

func (m mocTsspFileNilReadAt) ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int, ctx *immutable.ChunkMetaContext, ioPriority int) (*immutable.ChunkMeta, error) {
	if id == 0527 {
		return immutable.NewChunkMeta(0527, 0, 10, 1), nil
	}
	return nil, nil
}

func (m mocTsspFileNilReadAt) ReadAt(cm *immutable.ChunkMeta, segment int, dst *record.Record, decs *immutable.ReadContext, ioPriority int) (*record.Record, error) {
	return nil, nil
}

// TestFirstTimeInitMergeWithPool verifies the non-aggregate FirstTimeInit merge loop produces
// the same outRec as a direct MergeRecord reference, while reusing the dst builder from
// unorderPool. Two out-of-order files with an overlapping timestamp (t=2) exercise the merge
// and dedup path; the second-read file wins at the duplicate timestamp.
func TestFirstTimeInitMergeWithPool(t *testing.T) {
	// schema: [value:Int, time:Int] (time field is always last, see NewRecordSchema).
	schema := record.Schemas{
		{Type: influx.Field_Type_Int, Name: "value"},
		{Type: influx.Field_Type_Int, Name: record.TimeField},
	}
	files := []immutable.TSSPFile{
		mocTsspFileWithData{rows: []mocRow{{t: 2, v: 20}, {t: 3, v: 30}}},
		mocTsspFileWithData{rows: []mocRow{{t: 1, v: 10}, {t: 2, v: 99}}},
	}
	opt := &query.ProcessorOptions{Ascending: true, Limit: 100, StartTime: 0, EndTime: 10}
	qs := &executor.QuerySchema{}
	qs.SetOpt(opt)
	closedSignal := false
	ctx := &idKeyCursorContext{
		schema:       schema,
		querySchema:  qs,
		decs:         immutable.NewReadContext(qs.Options().IsAscending()),
		tr:           util.TimeRange{Min: 0, Max: 10},
		tmsMergePool: TsmMergePool,
		maxRowCnt:    100,
		readers:      &immutable.MmsReaders{OutOfOrders: files},
		closedSignal: &closedSignal,
	}

	cursor, err := newTsmMergeCursor(ctx, 0527, nil, nil, nil, false, nil)
	assert2.NoError(t, err)
	assert2.NotNil(t, cursor)

	rec, err := cursor.Next()
	assert2.NoError(t, err)
	assert2.NotNil(t, rec)

	// Reference: replicate the old merge with fresh records. Files are read in LocationCursor
	// order (file0 first as cumulative outRec, file1 second as newRec), matching FirstTimeInit.
	refDst0 := record.NewRecordBuilder(schema)
	rec0, _ := files[0].(mocTsspFileWithData).ReadAt(nil, 0, refDst0, nil, 0)
	refDst1 := record.NewRecordBuilder(schema)
	rec1, _ := files[1].(mocTsspFileWithData).ReadAt(nil, 0, refDst1, nil, 0)
	var ref record.Record
	ref.MergeRecord(rec1, rec0)

	assert2.Equal(t, ref.Times(), rec.Times())
	assert2.Equal(t, ref.ColVals[0].IntegerValues(), rec.ColVals[0].IntegerValues())
	// Sanity: the reference is the expected dedup (t=2 keeps the second file's value 99).
	assert2.Equal(t, []int64{1, 2, 3}, ref.Times())
	assert2.Equal(t, []int64{10, 99, 30}, ref.ColVals[0].IntegerValues())

	cursor.Close()
}

type mocRow struct {
	t    int64
	v    int64
	nilV bool
}

// mocTsspFileWithData returns a single-segment chunk whose ReadAt fills dst with the given rows.
// order distinguishes ordered vs out-of-order files; seq is the file sequence used for
// same-timestamp precedence (higher = newer = wins).
type mocTsspFileWithData struct {
	MocTsspFile
	rows  []mocRow
	order bool
	seq   uint64
}

func (m mocTsspFileWithData) IsOrder() bool                      { return m.order }
func (m mocTsspFileWithData) LevelAndSequence() (uint16, uint64) { return 0, m.seq }

func (m mocTsspFileWithData) ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int, ctx *immutable.ChunkMetaContext, ioPriority int) (*immutable.ChunkMeta, error) {
	if id == 0527 {
		minT, maxT := m.rows[0].t, m.rows[len(m.rows)-1].t
		return immutable.NewChunkMeta(0527, minT, maxT, 1), nil
	}
	return nil, nil
}

func (m mocTsspFileWithData) ReadAt(cm *immutable.ChunkMeta, segment int, dst *record.Record, decs *immutable.ReadContext, ioPriority int) (*record.Record, error) {
	for _, r := range m.rows {
		if r.nilV {
			dst.ColVals[0].AppendIntegerNull() // value column
		} else {
			dst.ColVals[0].AppendInteger(r.v)
		}
		dst.AppendTime(r.t) // time column (last)
	}
	return dst, nil
}

// mergeRow is a (time, value, isNil) tuple collected from cursor output for differential
// comparison between the eager and lazy paths.
type mergeRow struct {
	t     int64
	v     int64
	isNil bool
}

// runMergeCursorForTest builds a fresh tsmMergeCursor over the given ordered/unordered files and
// drains it to exhaustion, collecting (time, value, isNil) rows. maxRowCnt is intentionally
// configurable so callers can force small batches to exercise ready-buffer ownership.
func runMergeCursorForTest(t *testing.T, schema record.Schemas, ordered, unordered []immutable.TSSPFile, maxRowCnt int) []mergeRow {
	t.Helper()
	opt := &query.ProcessorOptions{Ascending: true, StartTime: 0, EndTime: 1 << 30}
	qs := &executor.QuerySchema{}
	qs.SetOpt(opt)
	closedSignal := false
	ctx := &idKeyCursorContext{
		schema:       schema,
		querySchema:  qs,
		decs:         immutable.NewReadContext(true),
		tr:           util.TimeRange{Min: 0, Max: 1 << 30},
		tmsMergePool: TsmMergePool,
		maxRowCnt:    maxRowCnt,
		readers:      &immutable.MmsReaders{Orders: ordered, OutOfOrders: unordered},
		closedSignal: &closedSignal,
	}
	cursor, err := newTsmMergeCursor(ctx, 0527, nil, nil, nil, false, nil)
	if err != nil {
		t.Fatalf("newTsmMergeCursor: %v", err)
	}
	if cursor == nil {
		return nil
	}
	var out []mergeRow
	for {
		rec, err := cursor.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if rec == nil {
			break
		}
		times := rec.Times()
		for i := 0; i < rec.RowNums(); i++ {
			v, isNil := rec.ColVals[0].IntegerValue(i)
			out = append(out, mergeRow{t: times[i], v: v, isNil: isNil})
		}
	}
	cursor.Close()
	return out
}

func buildFiles(files [][]mocRow, order bool) []immutable.TSSPFile {
	out := make([]immutable.TSSPFile, 0, len(files))
	for i, rows := range files {
		out = append(out, mocTsspFileWithData{rows: rows, order: order, seq: uint64(i + 1)})
	}
	return out
}

// TestLazyUnorderedMergeDifferential compares the lazy out-of-order merge path against the eager
// path (the oracle) across fixed edge cases and randomized scenarios. The lazy path is gated by
// the package flag; the eager path runs with the flag off.
func TestLazyUnorderedMergeDifferential(t *testing.T) {
	schema := record.Schemas{
		{Type: influx.Field_Type_Int, Name: "value"},
		{Type: influx.Field_Type_Int, Name: record.TimeField},
	}

	// Fixed edge cases. Each defines ordered and unordered row sets; both paths must produce the
	// same merged output.
	type tc struct {
		name      string
		ordered   [][]mocRow
		unordered [][]mocRow
		maxRows   int
	}
	cases := []tc{
		{
			name:      "ordered_only",
			ordered:   [][]mocRow{{{t: 10, v: 1}, {t: 11, v: 2}}},
			unordered: [][]mocRow{},
			maxRows:   100,
		},
		{
			name:      "unordered_only",
			ordered:   [][]mocRow{},
			unordered: [][]mocRow{{{t: 1, v: 1}, {t: 3, v: 3}}},
			maxRows:   100,
		},
		{
			// unordered files overlap each other at t=1; higher sequence wins (no ordered overlap).
			name:    "unordered_overlap_higher_seq_wins",
			ordered: [][]mocRow{{{t: 10, v: 1}, {t: 11, v: 5}}},
			unordered: [][]mocRow{
				{{t: 1, v: 2}, {t: 2, v: 4}}, // seq 1
				{{t: 1, v: 99}},              // seq 2, higher -> wins at t=1
			},
			maxRows: 100,
		},
		{
			name:    "nil_column_fills_from_older_unordered",
			ordered: [][]mocRow{{{t: 10, v: 1}}},
			unordered: [][]mocRow{
				{{t: 1, v: 10}},      // seq 1
				{{t: 1, nilV: true}}, // seq 2, nil -> older (seq 1) value 10 retained
			},
			maxRows: 100,
		},
		{
			name:      "disjoint_unordered_older",
			ordered:   [][]mocRow{{{t: 10, v: 1}, {t: 11, v: 2}}},
			unordered: [][]mocRow{{{t: 1, v: 10}, {t: 2, v: 11}}},
			maxRows:   100,
		},
		{
			name:    "small_batch_streaming",
			ordered: [][]mocRow{{{t: 10, v: 1}, {t: 11, v: 2}, {t: 12, v: 3}, {t: 13, v: 4}}},
			unordered: [][]mocRow{
				{{t: 1, v: 100}, {t: 3, v: 300}},
				{{t: 2, v: 200}, {t: 4, v: 400}},
			},
			maxRows: 2, // force the heap merger to stream in maxRowCnt batches
		},
		{
			name:    "multiple_ordered_files_after_unordered",
			ordered: [][]mocRow{{{t: 10, v: 1}}, {{t: 11, v: 5}}},
			unordered: [][]mocRow{
				{{t: 1, v: 11}, {t: 3, v: 33}, {t: 5, v: 55}, {t: 7, v: 77}},
			},
			maxRows: 100,
		},
	}

	runOne := func(c tc, lazy bool) []mergeRow {
		if lazy {
			SetLazyUnorderedMergeEnabled(true)
			prevThr := lazyUnorderedMergeMinLocations
			lazyUnorderedMergeMinLocations = 0 // bypass the small-N threshold to exercise lazy
			defer func() {
				SetLazyUnorderedMergeEnabled(false)
				lazyUnorderedMergeMinLocations = prevThr
			}()
		}
		ordered := buildFiles(c.ordered, true)
		unordered := buildFiles(c.unordered, false)
		return runMergeCursorForTest(t, schema, ordered, unordered, c.maxRows)
	}

	for _, c := range cases {
		eager := runOne(c, false)
		lazy := runOne(c, true)
		if !reflect.DeepEqual(eager, lazy) {
			t.Errorf("case %q mismatch\neager: %v\nlazy:  %v", c.name, eager, lazy)
		}
	}

	// Randomized differential testing on the real (disjoint) layout: unordered times in [1,20],
	// ordered times in [21,40]. Unordered files may overlap each other (dedup tested); ordered
	// and unordered never overlap (matches SplitRecordByTime's flush boundary).
	rng := rand.New(rand.NewSource(20240706))
	for iter := 0; iter < 3000; iter++ {
		c := tc{maxRows: 1 + rng.Intn(4)}
		nOrd := rng.Intn(3)
		for i := 0; i < nOrd; i++ {
			c.ordered = append(c.ordered, randomRows(rng, 1+rng.Intn(4), 21, 40))
		}
		nUnord := 1 + rng.Intn(4)
		for i := 0; i < nUnord; i++ {
			c.unordered = append(c.unordered, randomRows(rng, 1+rng.Intn(4), 1, 20))
		}
		eager := runOne(c, false)
		lazy := runOne(c, true)
		if !reflect.DeepEqual(eager, lazy) {
			t.Errorf("random case %d mismatch (ordered=%v unordered=%v maxRows=%d)\neager: %v\nlazy:  %v",
				iter, c.ordered, c.unordered, c.maxRows, eager, lazy)
		}
	}
}

// randomRows generates n rows with distinct ascending times in [minT, maxT], mimicking a real
// TSSP segment (time-sorted, distinct timestamps within a file). Some values are nil to exercise
// column-level nil merge. The real layout is disjoint (unordered older than ordered), so callers
// pass disjoint time ranges for ordered vs unordered; unordered files may overlap each other.
func randomRows(rng *rand.Rand, n, minT, maxT int) []mocRow {
	used := make(map[int64]bool, n)
	times := make([]int64, 0, n)
	span := maxT - minT + 1
	for len(times) < n {
		t := int64(minT + rng.Intn(span))
		if used[t] {
			continue
		}
		used[t] = true
		times = append(times, t)
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	rows := make([]mocRow, 0, n)
	for _, t := range times {
		rows = append(rows, mocRow{t: t, v: int64(rng.Intn(100)), nilV: rng.Intn(4) == 0})
	}
	return rows
}

// mocTsspFileMultiSeg models a file with multiple segments, each with its own rows and time
// range, so ReadDataBeforeWatermark's per-segment skip/defer logic can be exercised.
type mocTsspFileMultiSeg struct {
	MocTsspFile
	segs  [][]mocRow
	order bool
	seq   uint64
}

func (m mocTsspFileMultiSeg) IsOrder() bool                      { return m.order }
func (m mocTsspFileMultiSeg) LevelAndSequence() (uint16, uint64) { return 0, m.seq }

func (m mocTsspFileMultiSeg) ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int, ctx *immutable.ChunkMetaContext, ioPriority int) (*immutable.ChunkMeta, error) {
	if id == 0527 {
		ranges := make([]immutable.SegmentRange, len(m.segs))
		for i, seg := range m.segs {
			ranges[i] = immutable.SegmentRange{seg[0].t, seg[len(seg)-1].t}
		}
		return immutable.NewChunkMetaWithSegs(0527, ranges), nil
	}
	return nil, nil
}

func (m mocTsspFileMultiSeg) ReadAt(cm *immutable.ChunkMeta, segment int, dst *record.Record, decs *immutable.ReadContext, ioPriority int) (*record.Record, error) {
	if segment < 0 || segment >= len(m.segs) {
		return nil, nil
	}
	for _, r := range m.segs[segment] {
		if r.nilV {
			dst.ColVals[0].AppendIntegerNull()
		} else {
			dst.ColVals[0].AppendInteger(r.v)
		}
		dst.AppendTime(r.t)
	}
	return dst, nil
}

// TestLazyUnorderedMergeMultiSegment exercises ReadDataBeforeWatermark's per-segment skip/defer
// behavior with multi-segment files, where some segments fall at or before the ordered
// watermark and later segments must be deferred until the watermark advances. The eager path
// (oracle) reads every segment up front; the lazy path must produce identical output.
func TestLazyUnorderedMergeMultiSegment(t *testing.T) {
	schema := record.Schemas{
		{Type: influx.Field_Type_Int, Name: "value"},
		{Type: influx.Field_Type_Int, Name: record.TimeField},
	}

	cases := []struct {
		name      string
		ordered   []immutable.TSSPFile
		unordered []immutable.TSSPFile
		maxRows   int
	}{
		{
			// disjoint: unordered (older, multi-segment) then ordered (newer).
			name:    "multiseg_unordered_older_then_ordered",
			ordered: []immutable.TSSPFile{mocTsspFileWithData{rows: []mocRow{{t: 20, v: 1}, {t: 21, v: 6}, {t: 22, v: 11}}, order: true, seq: 1}},
			unordered: []immutable.TSSPFile{
				mocTsspFileMultiSeg{segs: [][]mocRow{
					{{t: 2, v: 20}, {t: 3, v: 30}},
					{{t: 5, v: 50}, {t: 7, v: 70}},
					{{t: 10, v: 100}, {t: 12, v: 120}},
				}, seq: 2},
			},
			maxRows: 100,
		},
		{
			name:    "multiseg_small_batch",
			ordered: []immutable.TSSPFile{mocTsspFileWithData{rows: []mocRow{{t: 20, v: 1}, {t: 21, v: 8}}, order: true, seq: 1}},
			unordered: []immutable.TSSPFile{
				mocTsspFileMultiSeg{segs: [][]mocRow{
					{{t: 2, v: 2}},
					{{t: 4, v: 4}},
					{{t: 6, v: 6}},
				}, seq: 2},
			},
			maxRows: 2,
		},
		{
			// two multi-seg unordered files overlapping each other (dedup), ordered newer.
			name:    "multiseg_two_files_overlap_each_other",
			ordered: []immutable.TSSPFile{mocTsspFileWithData{rows: []mocRow{{t: 20, v: 5}}, order: true, seq: 1}},
			unordered: []immutable.TSSPFile{
				mocTsspFileMultiSeg{segs: [][]mocRow{{{t: 1, v: 1}, {t: 5, v: 2}}}, seq: 2},
				mocTsspFileMultiSeg{segs: [][]mocRow{{{t: 5, v: 99}, {t: 9, v: 9}}}, seq: 3}, // t=5: higher seq wins
			},
			maxRows: 100,
		},
	}

	prevThr := lazyUnorderedMergeMinLocations
	lazyUnorderedMergeMinLocations = 0 // bypass the small-N threshold to exercise lazy
	defer func() { lazyUnorderedMergeMinLocations = prevThr }()
	for _, c := range cases {
		SetLazyUnorderedMergeEnabled(false)
		eager := runMergeCursorForTest(t, schema, c.ordered, c.unordered, c.maxRows)
		SetLazyUnorderedMergeEnabled(true)
		lazy := runMergeCursorForTest(t, schema, c.ordered, c.unordered, c.maxRows)
		SetLazyUnorderedMergeEnabled(false)
		if !reflect.DeepEqual(eager, lazy) {
			t.Errorf("case %q mismatch\neager: %v\nlazy:  %v", c.name, eager, lazy)
		}
	}
}
