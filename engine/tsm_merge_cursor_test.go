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

// TestFirstTimeInitNilOutRecNoPanic verifies that FirstTimeInit does not panic when every
// matched out-of-order location yields a nil record (here the mock ReadAt returns nil).
// Previously the span-count path called outRec.RowNums() on a nil outRec and panicked.
func TestFirstTimeInitNilOutRecNoPanic(t *testing.T) {
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
	// so outRec stays nil. The span-count path must not panic and Next must return nil.
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
	t, v int64
}

// mocTsspFileWithData returns a single-segment chunk whose ReadAt fills dst with the given rows.
type mocTsspFileWithData struct {
	MocTsspFile
	rows []mocRow
}

func (m mocTsspFileWithData) ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int, ctx *immutable.ChunkMetaContext, ioPriority int) (*immutable.ChunkMeta, error) {
	if id == 0527 {
		minT, maxT := m.rows[0].t, m.rows[len(m.rows)-1].t
		return immutable.NewChunkMeta(0527, minT, maxT, 1), nil
	}
	return nil, nil
}

func (m mocTsspFileWithData) ReadAt(cm *immutable.ChunkMeta, segment int, dst *record.Record, decs *immutable.ReadContext, ioPriority int) (*record.Record, error) {
	for _, r := range m.rows {
		dst.ColVals[0].AppendInteger(r.v) // value column
		dst.AppendTime(r.t)               // time column (last)
	}
	return dst, nil
}
