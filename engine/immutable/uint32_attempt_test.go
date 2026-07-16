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
	"errors"
	"math"
	"os"
	"sync/atomic"
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestUint32AttemptGuard_MsBuilderAbortRemovesCurrentFile(t *testing.T) {
	lock := ""
	builder := NewMsBuilder(t.TempDir(), "mst", &lock, NewTsStoreConfig(), 1,
		NewTSSPFileName(1, 0, 0, 0, true, &lock), util.Hot, nil, 1, config.TSSTORE, nil, 0)
	name := builder.fd.Name()
	require.FileExists(t, name)

	require.NoError(t, builder.Abort())
	_, err := os.Stat(name)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, builder.Abort())
}

func TestUint32AttemptGuard_MsBuilderCreateReturnsKnownIOError(t *testing.T) {
	lock := ""
	dir := t.TempDir()
	name := NewTSSPFileName(1, 0, 0, 0, true, &lock)
	first := NewMsBuilder(dir, "mst", &lock, NewTsStoreConfig(), 1,
		name, util.Hot, nil, 1, config.TSSTORE, nil, 0)
	t.Cleanup(func() { require.NoError(t, first.Abort()) })

	second, err := NewMsBuilderWithError(dir, "mst", &lock, NewTsStoreConfig(), 1,
		name, util.Hot, nil, 1, config.TSSTORE, nil, 0)
	require.Error(t, err)
	require.Nil(t, second)
}

func TestUint32AttemptGuard_ChunkIteratorInitErrorIsNotEOF(t *testing.T) {
	want := errno.NewError(errno.ErrCorruptTSSP, "broken metadata")
	itrs := &ChunkIterators{initErr: want}

	_, _, err := itrs.Next()
	require.Error(t, err)
	require.True(t, errno.Equal(err, errno.ErrCorruptTSSP))
}

func TestUint32AttemptGuard_StreamRetryDecision(t *testing.T) {
	require.True(t, shouldRetryWithStream(errno.NewError(errno.ErrRequireStream, 0, 2, 1), false))
	require.False(t, shouldRetryWithStream(errno.NewError(errno.ErrRequireStream, 0, 2, 1), true))
	require.False(t, shouldRetryWithStream(errno.NewError(errno.ErrCorruptTSSP, "bad range"), false))
	require.False(t, shouldRetryWithStream(errors.Join(
		errno.NewError(errno.ErrRequireStream, 0, 2, 1),
		&attemptAbortError{err: errors.New("remove temp failed")},
	), false))
}

func TestUint32AttemptGuard_SequencerUpdateDeferredUntilPublish(t *testing.T) {
	lock := ""
	seq := NewSequencer()
	builder := NewMsBuilder(t.TempDir(), "mst", &lock, NewTsStoreConfig(), 1,
		NewTSSPFileName(1, 0, 0, 0, true, &lock), util.Hot, seq, 1, config.TSSTORE, nil, 0)
	builder.DeferSequencerUpdate()

	rec := record.NewRecordBuilder(record.Schemas{
		{Name: "value", Type: influx.Field_Type_Int},
		{Name: record.TimeField, Type: influx.Field_Type_Int},
	})
	rec.ColVals[0].AppendInteger(7)
	rec.AppendTime(42)
	require.NoError(t, builder.WriteData(1, rec))

	file, err := builder.NewTSSPFile(true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Remove()) })

	lastFlush, _ := seq.Get("mst", 1)
	require.Equal(t, int64(math.MinInt64), lastFlush)

	builder.PublishSequencerUpdate()
	lastFlush, _ = seq.Get("mst", 1)
	require.Equal(t, int64(42), lastFlush)
}

func TestUint32AttemptGuard_DeferredHotFileAbortDoesNotChangeAccounting(t *testing.T) {
	manager := NewHotFileManager()
	before := atomic.LoadInt64(&manager.totalMemorySize)
	reader := &tsspFileReader{
		hot: true,
		r:   &HotFileReader{size: 128},
	}

	reader.FreeMemory()
	require.False(t, reader.hot)
	require.Equal(t, before, atomic.LoadInt64(&manager.totalMemorySize))
}

func TestUint32AttemptGuard_RemovePanicBecomesAbortError(t *testing.T) {
	file := &MockTSSPFileSeqIterator{RemoveFn: func() error {
		panic("close failed")
	}}

	err := RemoveTSSPFileOnAbort(file)
	require.EqualError(t, err, "remove temporary tssp file: close failed")
}
