/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package engine

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
)

type tsstoreImpl struct {
}

func (storage *tsstoreImpl) WriteCols(s *shard, cols *record.Record, mst string, binaryCols []byte) error {
	return errors.New("not implement yet")
}

func (storage *tsstoreImpl) WriteIndex(idx *tsi.IndexBuilder, mw *mstWriteCtx) func() error {
	err := storage.writeIndex(idx, mw)
	return func() error {
		return err
	}
}

func (storage *tsstoreImpl) writeIndex(idx *tsi.IndexBuilder, mw *mstWriteCtx) error {
	mmPoints := mw.getMstMap()
	var err error
	var writeIndexRequired bool
	start := time.Now()

	mergeSet, ok := idx.GetPrimaryIndex().(*tsi.MergeSetIndex)
	if !ok {
		return errno.NewInvalidTypeError("*tsi.MergeSetIndex", idx.GetPrimaryIndex())
	}

	for _, mp := range mmPoints.D {
		rows, ok := mp.Value.(*[]influx.Row)
		if !ok {
			return errors.New("can't map mmPoints")
		}

		for i := range *rows {
			ri := &(*rows)[i]

			if mergeSet.EnabledTagArray() && ri.HasTagArray() {
				writeIndexRequired = true
			}

			if !writeIndexRequired {
				ri.SeriesId, err = mergeSet.GetSeriesIdBySeriesKey(ri.IndexKey)
				if err != nil {
					return err
				}
				// PrimaryId is equal to SeriesId by default.
				ri.PrimaryId = ri.SeriesId

				if ri.SeriesId == 0 {
					writeIndexRequired = true
				}
			}
		}
	}

	failpoint.Inject("SlowDownCreateIndex", nil)
	if writeIndexRequired {
		if err = idx.CreateIndexIfNotExists(mmPoints, true); err != nil {
			return err
		}
	} else {
		if err = idx.CreateSecondaryIndexIfNotExist(mmPoints); err != nil {
			return err
		}
	}
	atomic.AddInt64(&statistics.PerfStat.WriteIndexDurationNs, time.Since(start).Nanoseconds())
	return nil
}

func (storage *tsstoreImpl) SetAccumulateMetaIndex(name string, detachedMetaInfo *immutable.AccumulateMetaIndex) {
}

func (storage *tsstoreImpl) shouldSnapshot(s *shard) bool {
	if s.snapshotTbl != nil {
		return s.snapshotErr != nil && !snapshotRetryBlocked(s.snapshotErr) && !s.forceFlushing()
	}
	if s.activeTbl == nil || s.forceFlushing() {
		return false
	}
	return true
}

func (storage *tsstoreImpl) timeToSnapshot(s *shard) bool {
	return fasttime.UnixTimestamp() >= (atomic.LoadUint64(&s.lastWriteTime) + s.writeColdDuration)
}

func (storage *tsstoreImpl) ForceFlush(s *shard) error {
	if s.indexBuilder == nil {
		return nil
	}
	s.enableForceFlush()
	defer s.disableForceFlush()

	s.waitSnapshot()
	s.prepareSnapshot()
	err := s.storage.writeSnapshot(s)
	s.endSnapshot()
	return err
}

func (storage *tsstoreImpl) writeSnapshot(s *shard) error {
	return storage.writeSnapshotGeneration(s, nil)
}

func (storage *tsstoreImpl) writeSnapshotGeneration(s *shard, expected *mutable.MemTable) error {
	s.snapshotCommitMu.Lock()
	defer s.snapshotCommitMu.Unlock()

	s.snapshotLock.RLock()
	blockedErr := s.snapshotErr
	s.snapshotLock.RUnlock()
	if snapshotRetryBlocked(blockedErr) {
		return blockedErr
	}

	if s.SnapShotter != nil {
		atomic.StoreUint32(&s.SnapShotter.RaftFlag, 0)
	}
	start := time.Now()
	rotated := false
	s.snapshotLock.Lock()
	if expected != nil && s.snapshotTbl == nil && s.activeTbl != expected {
		s.snapshotLock.Unlock()
		return nil
	}
	if expected != nil && s.snapshotTbl != nil && s.snapshotTbl != expected {
		err := s.snapshotErr
		s.snapshotLock.Unlock()
		if err != nil {
			return err
		}
		return nil
	}
	if s.snapshotTbl == nil && s.activeTbl == nil {
		s.snapshotLock.Unlock()
		return nil
	}
	if s.snapshotTbl == nil {
		walFiles, err := s.wal.Switch()
		if err != nil {
			s.snapshotLock.Unlock()
			return err
		}

		s.snapshotTbl = s.activeTbl
		s.snapshotWalFiles = walFiles
		s.snapshotSize = s.snapshotTbl.GetMemSize()
		s.snapshotCommitted = false
		s.snapshotErr = nil
		s.activeTbl = s.memTablePool.Get(s.engineType)
		s.activeTbl.SetIdx(s.skIdx)
		rotated = true
		if s.SnapShotter != nil {
			s.SnapShotter.RaftFlushC <- true
			atomic.StoreUint32(&s.SnapShotter.RaftFlag, 1)
		}
	}
	snapshot := s.snapshotTbl
	committed := s.snapshotCommitted
	walFiles := s.snapshotWalFiles
	s.snapshotLock.Unlock()

	if rotated {
		s.indexBuilder.Flush()
	}

	if !committed {
		if err := s.commitSnapshot(snapshot); err != nil {
			s.snapshotLock.Lock()
			s.snapshotErr = err
			s.snapshotLock.Unlock()
			return err
		}
		s.snapshotLock.Lock()
		s.snapshotCommitted = true
		s.snapshotLock.Unlock()
	}

	if err := RemoveWalFiles(walFiles); err != nil {
		s.snapshotLock.Lock()
		s.snapshotErr = err
		s.snapshotLock.Unlock()
		return err
	}
	if err := syncWalDirectories(walFiles); err != nil {
		s.snapshotLock.Lock()
		s.snapshotErr = err
		s.snapshotLock.Unlock()
		return err
	}
	if err := s.removeSnapshotManifest(); err != nil {
		s.snapshotLock.Lock()
		s.snapshotErr = err
		s.snapshotLock.Unlock()
		return err
	}

	//This fail point is used in scenarios where "s.snapshotTbl" is not recycled
	failpoint.Inject("snapshot-table-reset-delay", func() { time.Sleep(2 * time.Second) })

	s.snapshotLock.Lock()
	if s.snapshotTbl == snapshot {
		nodeMutableLimit.freeResource(s.snapshotSize)
		s.snapshotTbl.UnRef()
		s.snapshotTbl = nil
		s.snapshotWalFiles = nil
		s.snapshotSize = 0
		s.snapshotCommitted = false
		s.snapshotPrepared = nil
		s.snapshotErr = nil
	}
	s.snapshotLock.Unlock()

	atomic.AddInt64(&statistics.PerfStat.FlushSnapshotDurationNs, time.Since(start).Nanoseconds())
	atomic.AddInt64(&statistics.PerfStat.FlushSnapshotCount, 1)
	return nil
}

func syncWalDirectories(files *WalFiles) error {
	if files == nil {
		return nil
	}
	files.mu.Lock()
	dirs := make(map[string]struct{}, len(files.files))
	for _, name := range files.files {
		dirs[filepath.Dir(name)] = struct{}{}
	}
	files.mu.Unlock()

	var errs []error
	for dir := range dirs {
		if err := syncDirectory(dir); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (storage *tsstoreImpl) executeShardMove(s *shard) error {
	return s.doShardMove()
}

func (storage *tsstoreImpl) waitSnapshot() {}

func (storage *tsstoreImpl) getAllFiles(s *shard, mstName string) ([]immutable.TSSPFile, []string, error) {
	// get all order tssp files, since full compact is completed
	orderTsspFiles, existOrderFiles := s.immTables.GetTSSPFiles(mstName, true)
	outOfOrderTsspFiles, existOutOfOrderFiles := s.immTables.GetTSSPFiles(mstName, false)
	// has no both order and out of order files
	if !existOrderFiles && !existOutOfOrderFiles {
		return nil, nil, nil
	}

	var err error
	var initLen int
	if existOrderFiles {
		initLen += orderTsspFiles.Len()
	}
	if existOutOfOrderFiles {
		initLen += outOfOrderTsspFiles.Len()
	}
	allFiles := make([]immutable.TSSPFile, 0, initLen)
	coldTmpFilesPath := make([]string, 0, initLen)
	if existOrderFiles {
		orderTsspFiles.RLock()
		allFiles, coldTmpFilesPath, err = genAllFiles(s, orderTsspFiles.Files(), allFiles, coldTmpFilesPath)
		immutable.UnrefFilesReader(orderTsspFiles.Files()...)
		immutable.UnrefFiles(orderTsspFiles.Files()...)
		orderTsspFiles.RUnlock()
		if err != nil {
			return nil, nil, err
		}
	}

	if existOutOfOrderFiles {
		outOfOrderTsspFiles.RLock()
		allFiles, coldTmpFilesPath, err = genAllFiles(s, outOfOrderTsspFiles.Files(), allFiles, coldTmpFilesPath)
		immutable.UnrefFilesReader(outOfOrderTsspFiles.Files()...)
		immutable.UnrefFiles(outOfOrderTsspFiles.Files()...)
		outOfOrderTsspFiles.RUnlock()
		if err != nil {
			return nil, nil, err
		}
	}
	return allFiles, coldTmpFilesPath, nil
}
