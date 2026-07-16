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

package record

import (
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func useMaxVarColValBytes(t *testing.T, limit int64) {
	t.Helper()
	old := GetMaxVarColValBytes()
	require.NoError(t, SetMaxVarColValBytes(limit))
	t.Cleanup(func() {
		require.NoError(t, SetMaxVarColValBytes(old))
	})
}

func TestUint32OffsetGuard_TryAppendStringRejectsBeforeMutation(t *testing.T) {
	useMaxVarColValBytes(t, 8)

	var col ColVal
	col.AppendString("123456")

	err := col.TryAppendString("789")
	require.Error(t, err)
	require.True(t, errno.Equal(err, errno.ErrNeedFlush))
	require.Equal(t, []byte("123456"), col.Val)
	require.Equal(t, []uint32{0}, col.Offset)
	require.Equal(t, 1, col.Len)
}

func TestUint32OffsetGuard_AppendFieldsPreflightIsAtomic(t *testing.T) {
	schema := Schemas{
		{Name: "a", Type: influx.Field_Type_String},
		{Name: "b", Type: influx.Field_Type_String},
		{Name: TimeField, Type: influx.Field_Type_Int},
	}
	rec := NewRecordBuilder(schema)
	rec.ColVals[0].AppendString("aaa")
	rec.ColVals[1].AppendString("bbb")
	rec.TimeColumn().AppendInteger(1)

	useMaxVarColValBytes(t, 8)
	fields := []influx.Field{
		{Key: "a", Type: influx.Field_Type_String, StrValue: "1"},
		{Key: "b", Type: influx.Field_Type_String, StrValue: "123456"},
	}

	_, err := AppendFieldsToRecord(rec, fields, 2, true)
	require.Error(t, err)
	require.True(t, errno.Equal(err, errno.ErrNeedFlush))
	require.Equal(t, []byte("aaa"), rec.ColVals[0].Val)
	require.Equal(t, []byte("bbb"), rec.ColVals[1].Val)
	require.Equal(t, 1, rec.RowNums())
}

func TestUint32OffsetGuard_MergeRejectsBeforeMutation(t *testing.T) {
	schema := Schemas{
		{Name: "value", Type: influx.Field_Type_String},
		{Name: TimeField, Type: influx.Field_Type_Int},
	}
	left := NewRecordBuilder(schema)
	left.ColVals[0].AppendString("123456")
	left.TimeColumn().AppendInteger(1)
	right := NewRecordBuilder(schema)
	right.ColVals[0].AppendString("789")
	right.TimeColumn().AppendInteger(2)

	useMaxVarColValBytes(t, 8)
	err := left.TryMerge(right)
	require.Error(t, err)
	require.True(t, errno.Equal(err, errno.ErrRequireStream))
	require.Equal(t, []byte("123456"), left.ColVals[0].Val)
	require.Equal(t, 1, left.RowNums())
}

func TestUint32OffsetGuard_ValidateCorruptColumn(t *testing.T) {
	col := &ColVal{
		Val:      []byte("abc"),
		Offset:   []uint32{2, 1},
		Bitmap:   []byte{0xff},
		Len:      2,
		NilCount: 0,
	}

	err := ValidateCol(col, influx.Field_Type_String)
	require.Error(t, err)
	require.True(t, errno.Equal(err, errno.ErrCorruptColumn))
}
