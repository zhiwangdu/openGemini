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

package immutable

import (
	"math"
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestUint32ReaderGuard_ChunkEntryRangeSupportsControlledWrap(t *testing.T) {
	const chunkStart = int64(16)
	timeOffset := chunkStart + int64(math.MaxUint32) + 1
	actualSize := int64(math.MaxUint32) + 101
	cm := &ChunkMeta{
		offset:      chunkStart,
		size:        uint32(actualSize),
		columnCount: 2,
		segCount:    1,
		colMeta: []ColumnMeta{
			{name: "value", ty: influx.Field_Type_String, entries: []Segment{{offset: 20, size: 10}}},
			{name: record.TimeField, ty: influx.Field_Type_Int, entries: []Segment{{offset: timeOffset, size: 100}}},
		},
	}

	start, end, err := ChunkEntryRange(cm, chunkStart, chunkStart+actualSize)
	require.NoError(t, err)
	require.Equal(t, chunkStart, start)
	require.Equal(t, chunkStart+actualSize, end)
}

func TestUint32ReaderGuard_ChunkEntryRangeRejectsLowBitsMismatch(t *testing.T) {
	cm := &ChunkMeta{
		offset: 16,
		size:   3,
		colMeta: []ColumnMeta{
			{name: record.TimeField, ty: influx.Field_Type_Int, entries: []Segment{{offset: 20, size: 8}}},
		},
	}

	_, _, err := ChunkEntryRange(cm, 16, 64)
	require.Error(t, err)
	require.True(t, errno.Equal(err, errno.ErrCorruptTSSP))
}

func TestUint32ReaderGuard_PreloadRequiresEveryRequestedSegment(t *testing.T) {
	cm := &ChunkMeta{
		offset: 16,
		size:   100,
		colMeta: []ColumnMeta{
			{name: "value", ty: influx.Field_Type_String, entries: []Segment{{offset: 20, size: 10}}},
			{name: record.TimeField, ty: influx.Field_Type_Int, entries: []Segment{{offset: 120, size: 10}}},
		},
	}
	schema := record.Schemas{
		{Name: "value", Type: influx.Field_Type_String},
		{Name: record.TimeField, Type: influx.Field_Type_Int},
	}

	preload, err := canPreloadSegmentRecord(cm, 0, schema)
	require.NoError(t, err)
	require.False(t, preload)

	cm.colMeta[1].entries[0].offset = 100
	preload, err = canPreloadSegmentRecord(cm, 0, schema)
	require.NoError(t, err)
	require.True(t, preload)
}

func TestUint32ReaderGuard_CheckedColumnData(t *testing.T) {
	chunk := []byte("0123456789")
	got, err := columnData(chunk, 100, 102, 4)
	require.NoError(t, err)
	require.Equal(t, []byte("2345"), got)

	_, err = columnData(chunk, 100, 108, 4)
	require.Error(t, err)
	require.True(t, errno.Equal(err, errno.ErrCorruptTSSP))
}

func TestUint32ReaderGuard_WriteOriginalMetaUsesInt64Range(t *testing.T) {
	const sourceStart = int64(16)
	actualSize := int64(math.MaxUint32) + 101
	source := &ChunkMeta{
		offset: sourceStart,
		size:   uint32(actualSize),
		colMeta: []ColumnMeta{
			{name: record.TimeField, ty: influx.Field_Type_Int, preAgg: []byte{1, 2, 3}, entries: []Segment{{offset: sourceStart + int64(math.MaxUint32) + 1, size: 100}}},
		},
	}

	target, err := cloneChunkMetaForCopy(source, sourceStart, sourceStart+actualSize, 1_000)
	require.NoError(t, err)
	require.Equal(t, sourceStart, source.offset)
	require.Equal(t, sourceStart+int64(math.MaxUint32)+1, source.colMeta[0].entries[0].offset)
	require.Equal(t, int64(1_000), target.offset)
	require.Equal(t, int64(1_000)+int64(math.MaxUint32)+1, target.colMeta[0].entries[0].offset)
	require.Equal(t, uint32(100), target.size)
	require.Equal(t, []byte{1, 2, 3}, target.colMeta[0].preAgg)
	target.colMeta[0].preAgg[0] = 9
	require.Equal(t, byte(1), source.colMeta[0].preAgg[0])
}

func TestUint32ReaderGuard_NextChunkCopySizeDoesNotWrap(t *testing.T) {
	total := int64(math.MaxUint32) + 100
	copied := int64(math.MaxUint32) - 10

	size, err := nextChunkCopySize(copied, total, 128*1024)
	require.NoError(t, err)
	require.Equal(t, uint32(110), size)
}
