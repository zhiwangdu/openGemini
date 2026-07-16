// Copyright 2026 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mutable

import (
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/savsgio/dictpool"
	"github.com/stretchr/testify/require"
)

func TestUint32BatchGuard_RejectsWholeBatchBeforeMutation(t *testing.T) {
	oldLimit := record.GetMaxVarColValBytes()
	require.NoError(t, record.SetMaxVarColValBytes(10))
	t.Cleanup(func() { require.NoError(t, record.SetMaxVarColValBytes(oldLimit)) })

	table := NewMemTable(config.TSSTORE)
	ctx := WriteRowsCtx{AddRowCountsBySid: func(string, uint64, int64) {}}

	seed := []influx.Row{stringRow("mst", 1, 1, "123456")}
	require.NoError(t, writeTestRows(table, "mst", &seed, ctx))

	batch := []influx.Row{
		stringRow("mst", 1, 2, "abc"),
		stringRow("mst", 1, 3, "def"),
	}
	err := writeTestRows(table, "mst", &batch, ctx)
	require.Error(t, err)
	require.True(t, errno.Equal(err, errno.ErrNeedFlush))

	msInfo, getErr := table.GetMsInfo("mst")
	require.NoError(t, getErr)
	chunk := msInfo.sidMap[1]
	require.NotNil(t, chunk)
	require.Equal(t, 1, chunk.WriteRec.rec.RowNums())
	require.Equal(t, []string{"123456"}, chunk.WriteRec.rec.ColVals[0].StringValues(nil))
}

func TestUint32BatchGuard_RejectsSingleValueBeforeCreatingMeasurement(t *testing.T) {
	oldLimit := record.GetMaxVarColValBytes()
	require.NoError(t, record.SetMaxVarColValBytes(4))
	t.Cleanup(func() { require.NoError(t, record.SetMaxVarColValBytes(oldLimit)) })

	table := NewMemTable(config.TSSTORE)
	rows := []influx.Row{stringRow("mst", 1, 1, "12345")}
	err := writeTestRows(table, "mst", &rows, WriteRowsCtx{AddRowCountsBySid: func(string, uint64, int64) {}})
	require.Error(t, err)
	require.True(t, errno.Equal(err, errno.ErrValueTooLarge))
	require.Empty(t, table.msInfoMap)
}

func TestUint32BatchGuard_FlushValidationReturnsError(t *testing.T) {
	rec := &record.Record{
		Schema:  record.Schemas{{Name: "value", Type: influx.Field_Type_String}, {Name: record.TimeField, Type: influx.Field_Type_Int}},
		ColVals: []record.ColVal{{Val: []byte("x"), Offset: []uint32{2}, Len: 1, Bitmap: []byte{1}}, {Val: make([]byte, 8), Len: 1, Bitmap: []byte{1}}},
	}

	_, err := NewTsMemTableImpl().WriteRecordForFlush(rec, nil, nil, 1)
	require.Error(t, err)
	require.True(t, errno.Equal(err, errno.ErrCorruptColumn))
}

func writeTestRows(table *MemTable, mst string, rows *[]influx.Row, ctx WriteRowsCtx) error {
	var dict dictpool.Dict
	dict.Set(mst, rows)
	return table.MTable.WriteRows(table, &dict, ctx)
}

func stringRow(mst string, sid uint64, ts int64, value string) influx.Row {
	return influx.Row{
		Name:      mst,
		PrimaryId: sid,
		Timestamp: ts,
		Fields: influx.Fields{
			{Key: "value", StrValue: value, Type: influx.Field_Type_String},
		},
	}
}
