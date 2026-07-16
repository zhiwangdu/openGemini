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
	"fmt"
	"math"
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const (
	DefaultMaxVarColValBytes      int64 = 256 << 20
	MaxConfigurableVarColValBytes int64 = 512 << 20
)

var maxVarColValBytes atomic.Int64

func init() {
	maxVarColValBytes.Store(DefaultMaxVarColValBytes)
}

func GetMaxVarColValBytes() int64 {
	return maxVarColValBytes.Load()
}

func SetMaxVarColValBytes(limit int64) error {
	if limit <= 0 || limit > MaxConfigurableVarColValBytes {
		return fmt.Errorf("max variable-length column bytes must be in [1, %d], got %d", MaxConfigurableVarColValBytes, limit)
	}
	maxVarColValBytes.Store(limit)
	return nil
}

func CheckedUint32Size(size int64) (uint32, error) {
	if size < 0 || size > math.MaxUint32 {
		return 0, errno.NewError(errno.ErrSegmentTooLarge, size, uint64(math.MaxUint32))
	}
	return uint32(size), nil
}

func CanAppendVarBytes(current, appendBytes, limit int64) error {
	if current < 0 || appendBytes < 0 || limit <= 0 {
		return errno.NewError(errno.ErrCorruptColumn, "negative variable-length byte count or invalid limit")
	}
	if appendBytes > limit {
		return errno.NewError(errno.ErrValueTooLarge, appendBytes, limit)
	}
	if current > limit || appendBytes > limit-current {
		return errno.NewError(errno.ErrNeedFlush, current, appendBytes, limit)
	}
	return nil
}

func (cv *ColVal) VarBytes() int64 {
	if cv == nil {
		return 0
	}
	return int64(len(cv.Val))
}

func (cv *ColVal) VarBytesRange(start, end int) (int64, error) {
	if cv == nil || start < 0 || end < start || end > cv.Len || len(cv.Offset) != cv.Len {
		return 0, errno.NewError(errno.ErrCorruptColumn, "invalid string range or offset count")
	}
	if start == end {
		return 0, nil
	}
	if !cv.validStringOffsets() {
		return 0, errno.NewError(errno.ErrCorruptColumn, "invalid string offsets")
	}
	startOffset := int64(cv.Offset[start])
	endOffset := int64(len(cv.Val))
	if end < cv.Len {
		endOffset = int64(cv.Offset[end])
	}
	if endOffset < startOffset {
		return 0, errno.NewError(errno.ErrCorruptColumn, "string range end precedes start")
	}
	return endOffset - startOffset, nil
}

func (cv *ColVal) TryAppendString(v string) error {
	if err := CanAppendVarBytes(int64(len(cv.Val)), int64(len(v)), GetMaxVarColValBytes()); err != nil {
		return err
	}
	cv.AppendString(v)
	return nil
}

func (cv *ColVal) TryAppendStringNull() error {
	if err := CanAppendVarBytes(int64(len(cv.Val)), 0, GetMaxVarColValBytes()); err != nil {
		return err
	}
	cv.AppendStringNull()
	return nil
}

func (cv *ColVal) TryAppendColVal(src *ColVal, colType, start, end int) error {
	if colType == influx.Field_Type_String || colType == influx.Field_Type_Tag {
		appendBytes, err := src.VarBytesRange(start, end)
		if err != nil {
			return err
		}
		if err = CanAppendVarBytes(int64(len(cv.Val)), appendBytes, GetMaxVarColValBytes()); err != nil {
			return err
		}
	}
	cv.AppendColVal(src, colType, start, end)
	return nil
}

func (cv *ColVal) validStringOffsets() bool {
	if cv == nil || cv.Len < 0 || cv.NilCount < 0 || cv.NilCount > cv.Len || len(cv.Offset) != cv.Len {
		return false
	}
	if cv.Len == 0 {
		return len(cv.Val) == 0
	}
	if cv.Offset[0] != 0 {
		return false
	}
	valLen := uint64(len(cv.Val))
	for i := range cv.Offset {
		if uint64(cv.Offset[i]) > valLen {
			return false
		}
		if i > 0 && cv.Offset[i] < cv.Offset[i-1] {
			return false
		}
	}
	return true
}

func ValidateCol(col *ColVal, typ int) error {
	if col == nil {
		return errno.NewError(errno.ErrCorruptColumn, "nil column")
	}
	if col.Len < 0 || col.NilCount < 0 || col.NilCount > col.Len || col.BitMapOffset < 0 || col.BitMapOffset > 7 {
		return errno.NewError(errno.ErrCorruptColumn, "invalid length, nil count, or bitmap offset")
	}
	expectedBitmapLen := (col.BitMapOffset + col.Len + 7) / 8
	if len(col.Bitmap) != expectedBitmapLen {
		return errno.NewError(errno.ErrCorruptColumn, "invalid bitmap length")
	}
	if typ == influx.Field_Type_String || typ == influx.Field_Type_Tag {
		if !col.validStringOffsets() {
			return errno.NewError(errno.ErrCorruptColumn, "invalid string offsets")
		}
		if int64(len(col.Val)) > GetMaxVarColValBytes() {
			return errno.NewError(errno.ErrCorruptColumn, "string payload exceeds configured limit")
		}
		return nil
	}
	if typ < 0 || typ >= len(typeSize) {
		return errno.NewError(errno.ErrCorruptColumn, "unsupported column type")
	}
	expectedValueBytes := typeSize[typ] * (col.Len - col.NilCount)
	if expectedValueBytes != len(col.Val) {
		return errno.NewError(errno.ErrCorruptColumn, "invalid fixed-width value length")
	}
	return nil
}

func ValidateRecord(rec *Record) error {
	if rec == nil || len(rec.Schema) < 1 || len(rec.Schema) != len(rec.ColVals) {
		return errno.NewError(errno.ErrCorruptColumn, "record schema and column count mismatch")
	}
	if rec.Schema[len(rec.Schema)-1].Name != TimeField {
		return errno.NewError(errno.ErrCorruptColumn, "time column is not last")
	}
	rows := rec.ColVals[len(rec.ColVals)-1].Len
	for i := range rec.ColVals {
		if rec.ColVals[i].Len != rows {
			return errno.NewError(errno.ErrCorruptColumn, "record column row count mismatch")
		}
		if err := ValidateCol(&rec.ColVals[i], rec.Schema[i].Type); err != nil {
			return err
		}
		if i > 0 && rec.Schema[i-1].Name >= rec.Schema[i].Name && rec.Schema[i].Name != TimeField {
			return errno.NewError(errno.ErrCorruptColumn, "record schema is unordered or duplicated")
		}
	}
	if rec.ColVals[len(rec.ColVals)-1].NilCount != 0 {
		return errno.NewError(errno.ErrCorruptColumn, "time column contains null values")
	}
	return nil
}
