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

package engine

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestUint32SnapshotGuard_FailureRetainsSnapshotAndWal(t *testing.T) {
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, t.TempDir(), config.TSSTORE)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeShard(sh)) })

	rows := []influx.Row{{
		Name:      defaultMeasurementName,
		PrimaryId: 1,
		Timestamp: time.Now().UnixNano(),
		Fields: influx.Fields{
			{Key: "value", StrValue: "12345678", Type: influx.Field_Type_String},
		},
	}}
	require.NoError(t, writeData(sh, rows, false))

	oldLimit := record.GetMaxVarColValBytes()
	require.NoError(t, record.SetMaxVarColValBytes(4))
	err = sh.storage.writeSnapshot(sh)
	require.Error(t, err)
	require.True(t, errno.Equal(err, errno.ErrCorruptColumn), "unexpected error: %v", err)

	sh.snapshotLock.RLock()
	require.NotNil(t, sh.snapshotTbl)
	require.Error(t, sh.snapshotErr)
	require.False(t, sh.snapshotCommitted)
	sh.snapshotLock.RUnlock()
	require.True(t, snapshotRetryBlocked(err))

	require.NoError(t, record.SetMaxVarColValBytes(oldLimit))
	require.ErrorIs(t, sh.storage.writeSnapshot(sh), err)

	// Simulate explicit operator repair so the test can release the retained
	// generation. Permanent snapshot errors are never cleared automatically.
	sh.snapshotLock.Lock()
	sh.snapshotErr = nil
	sh.snapshotLock.Unlock()
	require.NoError(t, sh.storage.writeSnapshot(sh))
	sh.snapshotLock.RLock()
	require.Nil(t, sh.snapshotTbl)
	require.NoError(t, sh.snapshotErr)
	sh.snapshotLock.RUnlock()
}

func TestUint32SnapshotManifest_RollsPreparedGenerationForward(t *testing.T) {
	dir := t.TempDir()
	lock := ""
	temporary := filepath.Join(dir, "00000001.tssp.init")
	final := filepath.Join(dir, "00000001.tssp")
	wal := filepath.Join(dir, "1.wal")
	require.NoError(t, os.WriteFile(temporary, []byte("tssp"), 0o600))
	require.NoError(t, os.WriteFile(wal, []byte("wal"), 0o600))

	sh := &shard{dataPath: dir, lock: &lock}
	manifest := &snapshotCommitManifest{
		Version: snapshotManifestVersion,
		State:   snapshotManifestPrepared,
		Files:   []snapshotManifestFile{{Temporary: temporary, Final: final, Size: 4}},
		WAL:     []string{wal},
	}
	require.NoError(t, sh.writeSnapshotManifest(manifest))
	require.NoError(t, sh.recoverSnapshotCommit())
	require.FileExists(t, final)
	require.NoFileExists(t, temporary)
	require.FileExists(t, wal)
	require.FileExists(t, sh.snapshotManifestPath())
	require.NoError(t, sh.completeSnapshotCommitRecovery())
	require.NoFileExists(t, wal)
	require.NoFileExists(t, sh.snapshotManifestPath())
}

func TestUint32SnapshotManifest_ResumesAfterPartialRename(t *testing.T) {
	dir := t.TempDir()
	lock := ""
	firstFinal := filepath.Join(dir, "00000001.tssp")
	secondTemporary := filepath.Join(dir, "00000002.tssp.init")
	secondFinal := filepath.Join(dir, "00000002.tssp")
	wal := filepath.Join(dir, "1.wal")
	require.NoError(t, os.WriteFile(firstFinal, []byte("one"), 0o600))
	require.NoError(t, os.WriteFile(secondTemporary, []byte("two"), 0o600))
	require.NoError(t, os.WriteFile(wal, []byte("wal"), 0o600))

	sh := &shard{dataPath: dir, lock: &lock}
	require.NoError(t, sh.writeSnapshotManifest(&snapshotCommitManifest{
		Version: snapshotManifestVersion,
		State:   snapshotManifestPrepared,
		Files: []snapshotManifestFile{
			{Temporary: filepath.Join(dir, "00000001.tssp.init"), Final: firstFinal, Size: 3},
			{Temporary: secondTemporary, Final: secondFinal, Size: 3},
		},
		WAL: []string{wal},
	}))

	require.NoError(t, sh.recoverSnapshotCommit())
	require.FileExists(t, firstFinal)
	require.FileExists(t, secondFinal)
	require.NoFileExists(t, secondTemporary)
	require.FileExists(t, wal)
	require.FileExists(t, sh.snapshotManifestPath())
	require.NoError(t, sh.completeSnapshotCommitRecovery())
	require.NoFileExists(t, wal)
	require.NoFileExists(t, sh.snapshotManifestPath())
}

func TestUint32SnapshotManifest_MissingOutputFailsClosed(t *testing.T) {
	dir := t.TempDir()
	lock := ""
	wal := filepath.Join(dir, "1.wal")
	require.NoError(t, os.WriteFile(wal, []byte("wal"), 0o600))
	sh := &shard{dataPath: dir, lock: &lock}
	require.NoError(t, sh.writeSnapshotManifest(&snapshotCommitManifest{
		Version: snapshotManifestVersion,
		State:   snapshotManifestCommitted,
		Files:   []snapshotManifestFile{{Final: filepath.Join(dir, "missing.tssp"), Size: 4}},
		WAL:     []string{wal},
	}))

	require.Error(t, sh.recoverSnapshotCommit())
	require.FileExists(t, wal)
	require.FileExists(t, sh.snapshotManifestPath())
}

func TestUint32SnapshotGuard_NeedFlushRotatesGenerationAndRetriesBatch(t *testing.T) {
	oldLimit := record.GetMaxVarColValBytes()
	require.NoError(t, record.SetMaxVarColValBytes(10))
	t.Cleanup(func() { require.NoError(t, record.SetMaxVarColValBytes(oldLimit)) })

	sh, err := createShard(defaultDb, defaultRp, defaultPtId, t.TempDir(), config.TSSTORE)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeShard(sh)) })

	seed := []influx.Row{{
		Name: defaultMeasurementName, PrimaryId: 1, Timestamp: 1,
		Fields: influx.Fields{{Key: "value", StrValue: "123456", Type: influx.Field_Type_String}},
	}}
	require.NoError(t, writeData(sh, seed, false))
	firstGeneration := sh.activeTbl

	batch := []influx.Row{
		{Name: defaultMeasurementName, PrimaryId: 1, Timestamp: 2, Fields: influx.Fields{{Key: "value", StrValue: "abc", Type: influx.Field_Type_String}}},
		{Name: defaultMeasurementName, PrimaryId: 1, Timestamp: 3, Fields: influx.Fields{{Key: "value", StrValue: "def", Type: influx.Field_Type_String}}},
	}
	require.NoError(t, writeData(sh, batch, false))
	require.NotSame(t, firstGeneration, sh.activeTbl)
	require.Positive(t, sh.activeTbl.GetMemSize())
}

func TestUint32SnapshotGuard_ConcurrentWritersRotateGenerationOnce(t *testing.T) {
	oldLimit := record.GetMaxVarColValBytes()
	require.NoError(t, record.SetMaxVarColValBytes(10))
	t.Cleanup(func() { require.NoError(t, record.SetMaxVarColValBytes(oldLimit)) })

	sh, err := createShard(defaultDb, defaultRp, defaultPtId, t.TempDir(), config.TSSTORE)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeShard(sh)) })

	seed := []influx.Row{{
		Name: defaultMeasurementName, PrimaryId: 1, Timestamp: 1,
		Fields: influx.Fields{{Key: "value", StrValue: "12345678", Type: influx.Field_Type_String}},
	}}
	require.NoError(t, writeData(sh, seed, false))
	firstGeneration := sh.activeTbl

	start := make(chan struct{})
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(timestamp int64) {
			defer wg.Done()
			<-start
			errCh <- writeData(sh, []influx.Row{{
				Name: defaultMeasurementName, PrimaryId: 1, Timestamp: timestamp,
				Fields: influx.Fields{{Key: "value", StrValue: "abc", Type: influx.Field_Type_String}},
			}}, false)
		}(int64(i + 2))
	}
	close(start)
	wg.Wait()
	close(errCh)
	for writeErr := range errCh {
		require.NoError(t, writeErr)
	}

	require.NotSame(t, firstGeneration, sh.activeTbl)
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	require.NoError(t, err)
	sids := msInfo.GetAllSid()
	defer mutable.PutSidsImpl(sids)
	require.Len(t, sids, 1)
	chunk, exists := msInfo.CreateChunk(sids[0])
	require.True(t, exists)
	chunk.Mu.Lock()
	rows := chunk.WriteRec.GetRecord().RowNums()
	chunk.Mu.Unlock()
	require.Equal(t, 2, rows)
}

func TestUint32SnapshotGuard_AbortFailureBlocksAutomaticRetry(t *testing.T) {
	prepareErr := errors.New("prepare failed")
	cleanupErr := errors.New("remove temp failed")
	err := joinSnapshotAbortError(prepareErr, cleanupErr)

	require.ErrorIs(t, err, prepareErr)
	require.ErrorIs(t, err, cleanupErr)
	require.True(t, snapshotRetryBlocked(err))
	require.False(t, snapshotRetryBlocked(prepareErr))
}

func TestUint32SnapshotGuard_PermanentErrorIsNotAutomaticallyRetried(t *testing.T) {
	storage := &tsstoreImpl{}
	sh := &shard{
		storage:     storage,
		snapshotTbl: mutable.NewMemTable(config.TSSTORE),
		snapshotErr: errno.NewError(errno.ErrCorruptColumn, "invalid offsets"),
	}
	require.False(t, storage.shouldSnapshot(sh))
	require.False(t, sh.shouldSnapshot())

	sh.snapshotErr = errors.New("transient I/O failure")
	require.True(t, storage.shouldSnapshot(sh))
	require.True(t, sh.shouldSnapshot())
	sh.endSnapshot()
}

func TestUint32SnapshotGuard_WALRemovalIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	lock := ""
	name := filepath.Join(dir, "1.wal")
	require.NoError(t, os.WriteFile(name, []byte("wal"), 0o600))
	files := newWalFiles(1, &lock, dir)
	files.Add(name)

	require.NoError(t, RemoveWalFiles(files))
	require.NoError(t, RemoveWalFiles(files))
	require.NoError(t, syncWalDirectories(files))
}
