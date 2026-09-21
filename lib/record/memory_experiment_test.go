//go:build memdiag

package record

import (
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func TestLargeStringExperiment(t *testing.T) {
	var col ColVal
	values := []string{"", "a", strings.Repeat("x", 1023), strings.Repeat("a", 1024),
		strings.Repeat("q\"\\", 4096), strings.Repeat("b", 32768), strings.Repeat("c", 1048577)}
	for i := 0; i < 16; i++ {
		for _, value := range values {
			col.AppendString(value)
			col.AppendStringNull()
		}
	}
	for i := 0; i < col.Len; i += 2 {
		got, isNil := col.StringValueUnsafe(i)
		if isNil || got != values[(i/2)%len(values)] || !col.IsNil(i+1) {
			t.Fatalf("value or null changed at row %d", i)
		}
	}
	copyCol := ColVal{}
	copyCol.AppendColVal(&col, influx.Field_Type_String, 0, col.Len)
	col.Init()
	col.AppendString("overwrite old source")
	for i := 0; i < copyCol.Len; i += 2 {
		got, isNil := copyCol.StringValueUnsafe(i)
		if isNil || got != values[(i/2)%len(values)] {
			t.Fatalf("copy was not independent at row %d", i)
		}
	}
}

func TestMemoryGrowthGeometry(t *testing.T) {
	if testing.Short() {
		t.Skip("explicit allocation investigation")
	}
	value := strings.Repeat("x", 10240)
	var col ColVal
	col.AppendString("warmup")
	total := cap(col.Val)
	for i := 0; i < 26214; i++ {
		oldCap := cap(col.Val)
		col.AppendString(value)
		if cap(col.Val) != oldCap {
			total += cap(col.Val)
			t.Logf("row=%d len=%d old_cap=%d new_cap=%d cumulative_cap=%d", i+1, len(col.Val), oldCap, cap(col.Val), total)
		}
	}
	t.Logf("FINAL payload=%d len=%d cap=%d cumulative_cap=%d", 26214*10240, len(col.Val), cap(col.Val), total)
	runtime.KeepAlive(col)
}

// Blocks is a layout microbenchmark, not a complete implementation of segmented
// memtable records. It intentionally excludes sorting, queries and flush.
func BenchmarkLargeStringLayout(b *testing.B) {
	const rows = 3276
	const valueSize = 10240
	const blockBytes = 4 * 1024 * 1024
	value := strings.Repeat("x", valueSize)
	for _, layout := range []string{"growing", "batch_reserve_6", "batch_reserve_100", "reserve_exact", "blocks_4m"} {
		b.Run(layout, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(rows * valueSize)
			for n := 0; n < b.N; n++ {
				cols := make([]ColVal, 1)
				if layout == "reserve_exact" {
					cols[0].Val = make([]byte, 0, rows*valueSize)
				} else if layout == "blocks_4m" {
					cols[0].Val = make([]byte, 0, blockBytes)
				}
				for i := 0; i < rows; i++ {
					col := &cols[len(cols)-1]
					batchSize := 0
					if layout == "batch_reserve_6" {
						batchSize = 6
					} else if layout == "batch_reserve_100" {
						batchSize = 100
					}
					if batchSize > 0 && i%batchSize == 0 {
						col.Val = slices.Grow(col.Val, min(batchSize, rows-i)*valueSize)
					}
					if layout == "blocks_4m" && len(col.Val)+valueSize > blockBytes {
						cols = append(cols, ColVal{Val: make([]byte, 0, blockBytes)})
						col = &cols[len(cols)-1]
					}
					col.AppendString(value)
				}
				capacity := 0
				for _, col := range cols {
					capacity += cap(col.Val)
				}
				b.ReportMetric(float64(capacity)/float64(rows*valueSize), "capacity/payload")
				runtime.KeepAlive(cols)
			}
		})
	}
}

func TestLargeStringSortDeduplicate(t *testing.T) {
	rec := NewRecordBuilder(Schemas{
		{Type: influx.Field_Type_String, Name: "payload"},
		{Type: influx.Field_Type_Int, Name: TimeField},
	})
	times := []int64{3, 1, 2, 2, 4}
	for i, tm := range times {
		rec.ColVals[0].AppendString(strings.Repeat(fmt.Sprint(i), 10240))
		rec.AppendTime(tm)
	}
	hlp := NewColumnSortHelper()
	sorted := hlp.Sort(rec)
	defer hlp.Release()
	want := []int{1, 3, 0, 4}
	if sorted.RowNums() != len(want) {
		t.Fatalf("rows=%d", sorted.RowNums())
	}
	for i, index := range want {
		value, nilValue := sorted.ColVals[0].StringValueUnsafe(i)
		if nilValue || value != strings.Repeat(fmt.Sprint(index), 10240) {
			t.Fatalf("wrong duplicate resolution at row %d", i)
		}
	}
}
