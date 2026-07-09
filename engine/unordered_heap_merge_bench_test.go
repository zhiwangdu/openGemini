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
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

// benchSegments builds n disjoint out-of-order segment records of r rows each (segment i covers
// times [i*r, i*r+r)). Disjoint segments make the chain merge's accumulated outRec grow to n*r
// rows, exercising its O(N^2*R) rescan cost; the heap merge is O(M*logK) regardless. AppendColVal
// is read-only on its source, so the same records are reused across benchmark iterations.
func benchSegments(n, r int) []*record.Record {
	recs := make([]*record.Record, n)
	for i := 0; i < n; i++ {
		rows := make([]mergeRow, r)
		for j := 0; j < r; j++ {
			rows[j] = mergeRow{t: int64(i*r + j), v: int64(j)}
		}
		recs[i] = buildMergeRec(rows)
	}
	return recs
}

// BenchmarkUnorderedMerge compares the heap K-way merge against the chain MergeRecord loop over a
// range of out-of-order file counts (N) and per-file row counts (R). Expect the heap to be slower
// at small N (heap overhead), near parity around N=100, and much faster (with far fewer
// allocations) at large N as the chain's O(N^2*R) rescan dominates.
func BenchmarkUnorderedMerge(b *testing.B) {
	configs := []struct{ n, r int }{
		{10, 20}, {100, 20}, {1000, 20}, {10, 100}, {100, 100},
	}
	for _, cfg := range configs {
		recs := benchSegments(cfg.n, cfg.r)

		b.Run(fmt.Sprintf("heap_N%d_R%d", cfg.n, cfg.r), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sources := make([]*unorderedHeapSource, len(recs))
				for j, r := range recs {
					sources[j] = &unorderedHeapSource{rec: r, priority: j}
				}
				heapMergeUnordered(sources, heapMergeSchema, true, nil)
			}
		})

		b.Run(fmt.Sprintf("chain_N%d_R%d", cfg.n, cfg.r), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				chainMergeUnorderedRecords(recs, true)
			}
		})
	}
}

// --- core scenario: large string + many out-of-order files + high range overlap ---

var coreSchema = record.Schemas{
	{Type: influx.Field_Type_String, Name: "s"},
	{Type: influx.Field_Type_Int, Name: record.TimeField},
}

// benchCoreSegments builds nFiles out-of-order segment records of rowsPerFile rows each, every row
// carrying a str-sized string field. Timestamps are interleaved across files (file i row j ->
// j*nFiles + i), so all files span the same time range (near-full overlap) yet timestamps are
// distinct across files — the optimization's core scenario: large string + many out-of-order files
// + high range overlap without heavy timestamp duplication. 43200 out-of-order rows is half of the
// 86400-point workload (the other half is ordered, merged downstream by mergeData, unchanged).
func benchCoreSegments(nFiles, rowsPerFile int, str string) []*record.Record {
	recs := make([]*record.Record, nFiles)
	for i := 0; i < nFiles; i++ {
		rec := record.NewRecordBuilder(coreSchema)
		for j := 0; j < rowsPerFile; j++ {
			rec.ColVals[0].AppendString(str)
			rec.AppendTime(int64(j*nFiles + i))
		}
		recs[i] = rec
	}
	return recs
}

// BenchmarkUnorderedMergeCore measures heap vs chain on the core scenario across a string-size x
// point-count matrix. K (out-of-order file count) is fixed at 10; R = M/K. Peak live memory per
// combo is ~3-4*M*S (input + outRec + realloc temp); the largest combo (M=172800, S=9KB) peaks at
// ~4.8GB, so run with -benchtime=1x on a machine with ample RAM. The chain rescan copies each
// S-byte string O(K) times (O(K*M*S) total allocation), so it is expected to be far slower and
// far more allocation-heavy than the heap, which copies each string once (O(M*S)).
func BenchmarkUnorderedMergeCore(b *testing.B) {
	strKBSizes := []int{1, 3, 5, 7, 9}
	pointCounts := []int{86400, 129600, 172800} // out-of-order rows (half of the 86400/1.5x/2x workload)
	const K = 10
	for _, skb := range strKBSizes {
		str := strings.Repeat("a", skb*1024)
		for _, m := range pointCounts {
			r := m / K
			recs := benchCoreSegments(K, r, str)
			label := fmt.Sprintf("S%dkb_M%d", skb, m)

			b.Run("heap_"+label, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					sources := make([]*unorderedHeapSource, len(recs))
					for j, rc := range recs {
						sources[j] = &unorderedHeapSource{rec: rc, priority: j}
					}
					heapMergeUnordered(sources, coreSchema, true, nil)
				}
			})

			b.Run("chain_"+label, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					chainMergeUnorderedRecords(recs, true)
				}
			})
		}
	}
}

// BenchmarkUnorderedMergeKSweep varies the out-of-order file count K at fixed S=5KB, M=4320
// (M is reduced from the 86400 matrix so the chain's O(K*M*S) allocation stays feasible at K=300;
// live memory stays ~3*M*S = 65MB). Shows the chain's O(K^2*R)=O(K*M) blowup vs the heap's
// O(M*logK) as K grows.
func BenchmarkUnorderedMergeKSweep(b *testing.B) {
	str := strings.Repeat("a", 5*1024)
	const M = 4320
	for _, k := range []int{10, 50, 100, 300} {
		r := M / k
		recs := benchCoreSegments(k, r, str)
		label := fmt.Sprintf("K%d", k)

		b.Run("heap_"+label, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sources := make([]*unorderedHeapSource, len(recs))
				for j, rc := range recs {
					sources[j] = &unorderedHeapSource{rec: rc, priority: j}
				}
				heapMergeUnordered(sources, coreSchema, true, nil)
			}
		})

		b.Run("chain_"+label, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				chainMergeUnorderedRecords(recs, true)
			}
		})
	}
}
