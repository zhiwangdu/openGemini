// Copyright 2026 Huawei Cloud Computing Technologies Co., Ltd.
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

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const (
	benchTSMMergeMeasurement = "bench_tsm_merge_cursor"
	benchTSMMergeSid         = uint64(1001)
)

type tsmMergeCursorBenchData struct {
	readers *immutable.MmsReaders
	schema  record.Schemas
	tr      util.TimeRange
	files   []immutable.TSSPFile
}

func BenchmarkTsmMergeCursorFirstPacketWithManyUnorderedFiles(b *testing.B) {
	cases := []struct {
		name           string
		unorderedFiles int
		rowsPerFile    int
		fields         int
	}{
		{name: "unordered_files=1", unorderedFiles: 1, rowsPerFile: 128, fields: 4},
		{name: "unordered_files=10", unorderedFiles: 10, rowsPerFile: 128, fields: 4},
		{name: "unordered_files=100", unorderedFiles: 100, rowsPerFile: 128, fields: 4},
		{name: "unordered_files=1000", unorderedFiles: 1000, rowsPerFile: 128, fields: 4},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			data := buildTSMMergeCursorBenchData(b, tc.unorderedFiles, tc.rowsPerFile, tc.fields)
			defer closeTSMMergeCursorBenchData(b, data)

			b.ReportAllocs()
			b.ReportMetric(float64(tc.unorderedFiles), "unordered_files")
			b.ReportMetric(float64(tc.rowsPerFile), "rows_per_file")
			b.ReportMetric(float64(tc.fields), "fields")
			b.ResetTimer()

			var rows int
			for i := 0; i < b.N; i++ {
				cursor, ctx := newBenchTSMMergeCursor(b, data)
				rec, err := cursor.Next()
				if err != nil {
					b.Fatal(err)
				}
				if rec == nil || rec.RowNums() == 0 {
					b.Fatalf("expected first packet rows, got nil or empty record")
				}
				rows += rec.RowNums()
				closeBenchTSMMergeCursor(b, cursor, ctx)
			}

			b.ReportMetric(float64(rows)/float64(b.N), "first_rows/op")
		})
	}
}

func BenchmarkTsmMergeCursorFirstTimeInitWithManyUnorderedFiles(b *testing.B) {
	cases := []struct {
		name           string
		unorderedFiles int
		rowsPerFile    int
		fields         int
	}{
		{name: "unordered_files=1", unorderedFiles: 1, rowsPerFile: 128, fields: 4},
		{name: "unordered_files=10", unorderedFiles: 10, rowsPerFile: 128, fields: 4},
		{name: "unordered_files=100", unorderedFiles: 100, rowsPerFile: 128, fields: 4},
		{name: "unordered_files=1000", unorderedFiles: 1000, rowsPerFile: 128, fields: 4},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			data := buildTSMMergeCursorBenchData(b, tc.unorderedFiles, tc.rowsPerFile, tc.fields)
			defer closeTSMMergeCursorBenchData(b, data)

			b.ReportAllocs()
			b.ReportMetric(float64(tc.unorderedFiles), "unordered_files")
			b.ReportMetric(float64(tc.rowsPerFile), "rows_per_file")
			b.ReportMetric(float64(tc.fields), "fields")
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				cursor, ctx := newBenchTSMMergeCursor(b, data)
				if err := cursor.FirstTimeInit(); err != nil {
					b.Fatal(err)
				}
				if cursor.outOrderRecIter.record == nil || cursor.outOrderRecIter.record.RowNums() == 0 {
					b.Fatalf("expected unordered outRec rows, got nil or empty record")
				}
				closeBenchTSMMergeCursor(b, cursor, ctx)
			}
		})
	}
}

func buildTSMMergeCursorBenchData(b *testing.B, unorderedFiles, rowsPerFile, fields int) *tsmMergeCursorBenchData {
	b.Helper()

	dir := b.TempDir()
	lockPath := ""
	conf := immutable.NewTsStoreConfig()
	conf.SetMaxRowsPerSegment(rowsPerFile)
	conf.SetMaxSegmentLimit(1)
	conf.SetFilesLimit(1 << 30)

	schema := newTSMMergeCursorBenchSchema(fields)
	readers := &immutable.MmsReaders{}
	files := make([]immutable.TSSPFile, 0, unorderedFiles+1)

	seq := uint64(1)
	orderFile := writeTSMMergeCursorBenchFile(b, dir, &lockPath, conf, seq, true, benchTSMMergeSid,
		newTSMMergeCursorBenchRecord(schema, 0, rowsPerFile, fields, 1))
	readers.Orders = append(readers.Orders, orderFile)
	files = append(files, orderFile)
	seq++

	for i := 0; i < unorderedFiles; i++ {
		startTime := int64(i * rowsPerFile)
		rec := newTSMMergeCursorBenchRecord(schema, startTime, rowsPerFile, fields, int64(i+2))
		file := writeTSMMergeCursorBenchFile(b, dir, &lockPath, conf, seq, false, benchTSMMergeSid, rec)
		readers.OutOfOrders = append(readers.OutOfOrders, file)
		files = append(files, file)
		seq++
	}

	return &tsmMergeCursorBenchData{
		readers: readers,
		schema:  schema,
		tr:      util.TimeRange{Min: 0, Max: int64(unorderedFiles*rowsPerFile + rowsPerFile)},
		files:   files,
	}
}

func closeTSMMergeCursorBenchData(b *testing.B, data *tsmMergeCursorBenchData) {
	b.Helper()

	for _, file := range data.files {
		if err := file.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func newBenchTSMMergeCursor(b *testing.B, data *tsmMergeCursorBenchData) (*tsmMergeCursor, *idKeyCursorContext) {
	b.Helper()

	opt := &query.ProcessorOptions{
		Name:      benchTSMMergeMeasurement,
		Ascending: true,
		StartTime: data.tr.Min,
		EndTime:   data.tr.Max,
	}
	querySchema := genQuerySchema(nil, opt)
	ctx := &idKeyCursorContext{
		maxRowCnt:    1000,
		engineType:   config.TSSTORE,
		tr:           data.tr,
		queryTr:      data.tr,
		interTr:      data.tr,
		schema:       data.schema,
		readers:      data.readers,
		querySchema:  querySchema,
		decs:         immutable.NewReadContext(true),
		metaContext:  immutable.NewChunkMetaContext(data.schema),
		tmsMergePool: record.NewRecordPool(record.TsmMergePool),
	}
	ctx.decs.Set(true, data.tr, false, nil)

	cursor, err := newTsmMergeCursor(ctx, benchTSMMergeSid, nil, nil, nil, false, nil)
	if err != nil {
		b.Fatal(err)
	}
	if cursor == nil {
		b.Fatal("expected tsmMergeCursor, got nil")
	}
	return cursor, ctx
}

func closeBenchTSMMergeCursor(b *testing.B, cursor *tsmMergeCursor, ctx *idKeyCursorContext) {
	b.Helper()

	if err := cursor.Close(); err != nil {
		b.Fatal(err)
	}
	if ctx.decs != nil {
		ctx.decs.Release()
	}
	if ctx.metaContext != nil {
		ctx.metaContext.Release()
	}
}

func writeTSMMergeCursorBenchFile(b *testing.B, dir string, lockPath *string, conf *immutable.Config, seq uint64, order bool, sid uint64, rec *record.Record) immutable.TSSPFile {
	b.Helper()

	fileName := immutable.NewTSSPFileName(seq, 0, 0, 0, order, lockPath)
	builder := immutable.NewMsBuilder(dir, benchTSMMergeMeasurement, lockPath, conf, 1, fileName, util.Hot, nil, rec.RowNums(), config.TSSTORE, nil, 0)
	if err := builder.WriteData(sid, rec); err != nil {
		b.Fatal(err)
	}
	file, err := builder.NewTSSPFile(false)
	if err != nil {
		b.Fatal(err)
	}
	if file == nil {
		b.Fatal("expected tssp file, got nil")
	}
	return file
}

func newTSMMergeCursorBenchSchema(fields int) record.Schemas {
	schema := make(record.Schemas, 0, fields+1)
	for i := 0; i < fields; i++ {
		schema = append(schema, record.Field{
			Name: fmt.Sprintf("field_%02d", i),
			Type: influx.Field_Type_Int,
		})
	}
	schema = append(schema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
	return schema
}

func newTSMMergeCursorBenchRecord(schema record.Schemas, startTime int64, rows, fields int, valueBase int64) *record.Record {
	rec := record.NewRecordBuilder(schema)
	for row := 0; row < rows; row++ {
		for field := 0; field < fields; field++ {
			rec.Column(field).AppendInteger(valueBase + int64(field))
		}
		rec.TimeColumn().AppendInteger(startTime + int64(row))
	}
	return rec
}
