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
