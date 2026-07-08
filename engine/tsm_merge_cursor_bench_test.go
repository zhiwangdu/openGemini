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
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

// benchSchema is [value:Int, time:Int].
var benchSchema = record.Schemas{
	{Type: influx.Field_Type_Int, Name: "value"},
	{Type: influx.Field_Type_Int, Name: record.TimeField},
}

// makeBenchFiles builds one ordered file with rows at times [1..rowsPerFile] (so the first
// ordered batch watermark = rowsPerFile) and nUnordered out-of-order files whose rows all start
// at unorderedStart (well beyond the first watermark when unorderedStart >> rowsPerFile), so the
// lazy path defers all of them on the first packet while the eager path reads every one up front.
func makeBenchFiles(nUnordered, rowsPerFile int, unorderedStart int64) ([]immutable.TSSPFile, []immutable.TSSPFile) {
	orderedRows := make([]mocRow, rowsPerFile)
	for i := 0; i < rowsPerFile; i++ {
		orderedRows[i] = mocRow{t: int64(i + 1), v: int64(i + 1)}
	}
	ordered := []immutable.TSSPFile{mocTsspFileWithData{rows: orderedRows, order: true, seq: 1}}

	unordered := make([]immutable.TSSPFile, nUnordered)
	for i := 0; i < nUnordered; i++ {
		rows := make([]mocRow, rowsPerFile)
		for j := 0; j < rowsPerFile; j++ {
			t := unorderedStart + int64(i)*int64(rowsPerFile) + int64(j)
			rows[j] = mocRow{t: t, v: int64(i*1000 + j)}
		}
		unordered[i] = mocTsspFileWithData{rows: rows, order: false, seq: uint64(i + 1)}
	}
	return ordered, unordered
}

// makeOverlapBenchFiles builds unordered files whose rows overlap the ordered file's time range,
// so the lazy path must read all of them for the first packet (no deferral benefit); this
// isolates the merge-algorithm cost (lazy K-way vs eager chain merge).
func makeOverlapBenchFiles(nUnordered, rowsPerFile int) ([]immutable.TSSPFile, []immutable.TSSPFile) {
	orderedRows := make([]mocRow, rowsPerFile)
	for i := 0; i < rowsPerFile; i++ {
		orderedRows[i] = mocRow{t: int64(i + 1), v: int64(i + 1)}
	}
	ordered := []immutable.TSSPFile{mocTsspFileWithData{rows: orderedRows, order: true, seq: 1}}

	unordered := make([]immutable.TSSPFile, nUnordered)
	for i := 0; i < nUnordered; i++ {
		rows := make([]mocRow, rowsPerFile)
		for j := 0; j < rowsPerFile; j++ {
			// all unordered rows fall inside the ordered time range [1, rowsPerFile]
			rows[j] = mocRow{t: int64(j + 1), v: int64(i*1000 + j)}
		}
		unordered[i] = mocTsspFileWithData{rows: rows, order: false, seq: uint64(i + 1)}
	}
	return ordered, unordered
}

func benchCtx(ordered, unordered []immutable.TSSPFile) *idKeyCursorContext {
	opt := &query.ProcessorOptions{Ascending: true, StartTime: 0, EndTime: 1 << 30}
	qs := &executor.QuerySchema{}
	qs.SetOpt(opt)
	closedSignal := false
	return &idKeyCursorContext{
		schema:       benchSchema,
		querySchema:  qs,
		decs:         immutable.NewReadContext(true),
		tr:           util.TimeRange{Min: 0, Max: 1 << 30},
		tmsMergePool: TsmMergePool,
		maxRowCnt:    1000,
		readers:      &immutable.MmsReaders{Orders: ordered, OutOfOrders: unordered},
		closedSignal: &closedSignal,
	}
}

// benchFirstPacket times a single Next() call (FirstTimeInit + first batch). Cursor creation
// (AddLoc) is excluded via StopTimer so the measurement isolates the first-packet cost.
func benchFirstPacket(b *testing.B, ordered, unordered []immutable.TSSPFile, lazy bool) {
	SetLazyUnorderedMergeEnabled(lazy)
	prevThr := lazyUnorderedMergeMinLocations
	if lazy {
		lazyUnorderedMergeMinLocations = 0 // bypass the small-N threshold to measure the lazy path itself
	}
	defer func() {
		SetLazyUnorderedMergeEnabled(false)
		lazyUnorderedMergeMinLocations = prevThr
	}()
	b.ReportAllocs()
	ctx := benchCtx(ordered, unordered)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cursor, err := newTsmMergeCursor(ctx, 0527, nil, nil, nil, false, nil)
		if err != nil {
			b.Fatal(err)
		}
		if cursor == nil {
			b.Skip("cursor is nil")
		}
		b.StartTimer()
		if _, err := cursor.Next(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		cursor.Close()
		b.StartTimer()
	}
}

// benchTotal times draining the cursor to exhaustion (total query cost, not just first packet).
func benchTotal(b *testing.B, ordered, unordered []immutable.TSSPFile, lazy bool) {
	SetLazyUnorderedMergeEnabled(lazy)
	prevThr := lazyUnorderedMergeMinLocations
	if lazy {
		lazyUnorderedMergeMinLocations = 0 // bypass the small-N threshold to measure the lazy path itself
	}
	defer func() {
		SetLazyUnorderedMergeEnabled(false)
		lazyUnorderedMergeMinLocations = prevThr
	}()
	b.ReportAllocs()
	ctx := benchCtx(ordered, unordered)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cursor, err := newTsmMergeCursor(ctx, 0527, nil, nil, nil, false, nil)
		if err != nil {
			b.Fatal(err)
		}
		if cursor == nil {
			b.Skip("cursor is nil")
		}
		b.StartTimer()
		for {
			rec, err := cursor.Next()
			if err != nil {
				b.Fatal(err)
			}
			if rec == nil {
				break
			}
		}
		b.StopTimer()
		cursor.Close()
		b.StartTimer()
	}
}

// BenchmarkFirstPacket_NoOverlap measures first-packet latency when out-of-order files are all
// beyond the first ordered watermark. The eager path reads every out-of-order file up front
// (O(N) reads + chain merge); the lazy path defers them all and reads only the ordered batch.
// This is the optimization's headline scenario (first-packet latency vs N unordered files).
func BenchmarkFirstPacket_NoOverlap(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		ordered, unordered := makeBenchFiles(n, 20, 1000)
		b.Run(fmt.Sprintf("Eager_N%d", n), func(b *testing.B) { benchFirstPacket(b, ordered, unordered, false) })
		b.Run(fmt.Sprintf("Lazy_N%d", n), func(b *testing.B) { benchFirstPacket(b, ordered, unordered, true) })
	}
}

// BenchmarkFirstPacket_FullOverlap measures first-packet latency when out-of-order files fully
// overlap the ordered time range. The lazy path cannot defer any data, so both paths read all
// out-of-order files for the first packet; this isolates the per-iteration merge/alloc cost.
func BenchmarkFirstPacket_FullOverlap(b *testing.B) {
	for _, n := range []int{10, 100} {
		ordered, unordered := makeOverlapBenchFiles(n, 20)
		b.Run(fmt.Sprintf("Eager_N%d", n), func(b *testing.B) { benchFirstPacket(b, ordered, unordered, false) })
		b.Run(fmt.Sprintf("Lazy_N%d", n), func(b *testing.B) { benchFirstPacket(b, ordered, unordered, true) })
	}
}

// BenchmarkTotal_NoOverlap measures total drain time (all data eventually read) for the
// no-overlap scenario. Both paths read all out-of-order data; the lazy path spreads the work
// across batches. Shows the end-to-end cost including lazy planner overhead.
//
// NOTE: makeBenchFiles puts ordered EARLY (t=1..) and unordered LATE (t=1000+), which is the
// OPPOSITE of openGemini's real layout (unordered is older, ordered is newer). It is kept only
// as an algorithmic micro-bench. For the real-layout numbers see BenchmarkRealLayout_*.
func BenchmarkTotal_NoOverlap(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		ordered, unordered := makeBenchFiles(n, 20, 1000)
		b.Run(fmt.Sprintf("Eager_N%d", n), func(b *testing.B) { benchTotal(b, ordered, unordered, false) })
		b.Run(fmt.Sprintf("Lazy_N%d", n), func(b *testing.B) { benchTotal(b, ordered, unordered, true) })
	}
}

// makeRealLayoutBenchFiles models openGemini's actual layout: ordered data is NEWER, unordered
// data is OLDER (out-of-order writes carry older timestamps; SplitRecordByTime at flush splits at
// the persisted ordered max). Ordered file covers [orderedStart, orderedStart+R); the N unordered
// files cover the older range [1, N*R] < orderedStart.
func makeRealLayoutBenchFiles(nUnordered, rowsPerFile int, orderedStart int64) ([]immutable.TSSPFile, []immutable.TSSPFile) {
	orderedRows := make([]mocRow, rowsPerFile)
	for i := 0; i < rowsPerFile; i++ {
		orderedRows[i] = mocRow{t: orderedStart + int64(i), v: int64(i + 1)}
	}
	ordered := []immutable.TSSPFile{mocTsspFileWithData{rows: orderedRows, order: true, seq: 1}}

	unordered := make([]immutable.TSSPFile, nUnordered)
	for i := 0; i < nUnordered; i++ {
		rows := make([]mocRow, rowsPerFile)
		for j := 0; j < rowsPerFile; j++ {
			// file i covers [i*R+1, (i+1)*R], all < orderedStart (older)
			t := int64(i*rowsPerFile + j + 1)
			rows[j] = mocRow{t: t, v: int64(i*1000 + j)}
		}
		unordered[i] = mocTsspFileWithData{rows: rows, order: false, seq: uint64(i + 1)}
	}
	return ordered, unordered
}

// BenchmarkRealLayout_Total measures total drain time under openGemini's real layout (unordered
// older, ordered newer). For an ascending query the output is unordered(older) -> ordered(newer);
// the lazy watermark = ordered batch max covers ALL older unordered, so lazy admits everything in
// the first batch -> no deferral. Expected: lazy ~= eager (no 5x win; possibly worse from heap
// overhead), confirming the ascending watermark design is broken for the real layout.
func BenchmarkRealLayout_Total(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		ordered, unordered := makeRealLayoutBenchFiles(n, 20, 1<<20) // ordered far in the future
		b.Run(fmt.Sprintf("Eager_N%d", n), func(b *testing.B) { benchTotal(b, ordered, unordered, false) })
		b.Run(fmt.Sprintf("Lazy_N%d", n), func(b *testing.B) { benchTotal(b, ordered, unordered, true) })
	}
}

// BenchmarkRealLayout_FirstPacket measures the first Next() under the real layout. The lazy path
// reads the ordered batch first (watermark = its max, which is the newest time), admits all older
// unordered, and merges -> first packet cost ~= eager (reads all unordered up front).
func BenchmarkRealLayout_FirstPacket(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		ordered, unordered := makeRealLayoutBenchFiles(n, 20, 1<<20)
		b.Run(fmt.Sprintf("Eager_N%d", n), func(b *testing.B) { benchFirstPacket(b, ordered, unordered, false) })
		b.Run(fmt.Sprintf("Lazy_N%d", n), func(b *testing.B) { benchFirstPacket(b, ordered, unordered, true) })
	}
}
