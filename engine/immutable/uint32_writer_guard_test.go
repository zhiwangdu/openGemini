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

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestUint32WriterGuard_SegmentSizeCheckedBeforeCast(t *testing.T) {
	size, err := checkedSegmentSize(int64(math.MaxUint32))
	require.NoError(t, err)
	require.Equal(t, uint32(math.MaxUint32), size)

	_, err = checkedSegmentSize(int64(math.MaxUint32) + 1)
	require.Error(t, err)
	require.True(t, errno.Equal(err, errno.ErrSegmentTooLarge))
}

func TestUint32WriterGuard_ChunkMetaUsesLowBitsButCursorUsesWriterPosition(t *testing.T) {
	const chunkStart = int64(16)
	actualChunkSize := int64(math.MaxUint32) + 101
	writerPosition := chunkStart + actualChunkSize
	cm := &ChunkMeta{}

	require.NoError(t, setChunkMetaSize(cm, actualChunkSize))
	require.Equal(t, uint32(100), cm.size)

	next, err := nextChunkDataOffset(chunkStart, writerPosition, cm)
	require.NoError(t, err)
	require.Equal(t, writerPosition, next)
	require.NotEqual(t, chunkStart+int64(cm.size), next)
}

func TestUint32WriterGuard_ChunkCursorRejectsInconsistentLowBits(t *testing.T) {
	cm := &ChunkMeta{size: 7}
	_, err := nextChunkDataOffset(16, 32, cm)
	require.Error(t, err)
	require.True(t, errno.Equal(err, errno.ErrCorruptTSSP))
}

func TestUint32WriterGuard_MsBuilderUsesPhysicalCursorAfterHeader(t *testing.T) {
	lock := ""
	builder := NewMsBuilder(t.TempDir(), "mst", &lock, NewTsStoreConfig(), 2,
		NewTSSPFileName(1, 0, 0, 0, true, &lock), util.Hot, nil, 1, config.TSSTORE, nil, 0)
	t.Cleanup(func() { require.NoError(t, builder.Abort()) })

	rec := record.NewRecordBuilder(record.Schemas{
		{Name: "value", Type: influx.Field_Type_Int},
		{Name: record.TimeField, Type: influx.Field_Type_Int},
	})
	rec.ColVals[0].AppendInteger(7)
	rec.AppendTime(42)

	require.NoError(t, builder.WriteData(1, rec))
	first := builder.chunkBuilder.chunkMeta.Clone()
	firstEnd := builder.dataOffset
	require.Equal(t, first.offset+int64(first.size), firstEnd)

	require.NoError(t, builder.WriteData(2, rec))
	second := builder.chunkBuilder.chunkMeta.Clone()
	require.Equal(t, firstEnd, second.offset)
	require.Equal(t, second.offset+int64(second.size), builder.dataOffset)
}
