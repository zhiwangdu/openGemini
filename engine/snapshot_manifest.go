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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/openGemini/openGemini/lib/fileops"
)

const (
	snapshotManifestVersion   = 1
	snapshotManifestFileName  = ".snapshot-commit.json"
	snapshotManifestPrepared  = "prepared"
	snapshotManifestCommitted = "committed"
)

type snapshotManifestFile struct {
	Temporary string `json:"temporary"`
	Final     string `json:"final"`
	Size      int64  `json:"size"`
}

type snapshotCommitManifest struct {
	Version int                    `json:"version"`
	State   string                 `json:"state"`
	Files   []snapshotManifestFile `json:"files"`
	WAL     []string               `json:"wal"`
}

func (s *shard) newSnapshotCommitManifest(prepared []preparedSnapshotMeasurement, wal *WalFiles) (*snapshotCommitManifest, error) {
	manifest := &snapshotCommitManifest{Version: snapshotManifestVersion, State: snapshotManifestPrepared}
	seen := make(map[string]struct{})
	for i := range prepared {
		for _, file := range prepared[i].flush.SnapshotFiles() {
			if file.Size <= 0 || file.Temporary == "" || file.Final == "" {
				return nil, errors.New("invalid prepared snapshot output")
			}
			if _, ok := seen[file.Final]; ok {
				return nil, fmt.Errorf("duplicate snapshot output %q", file.Final)
			}
			seen[file.Final] = struct{}{}
			manifest.Files = append(manifest.Files, snapshotManifestFile{
				Temporary: file.Temporary,
				Final:     file.Final,
				Size:      file.Size,
			})
		}
	}
	if wal != nil {
		wal.mu.Lock()
		manifest.WAL = append(manifest.WAL, wal.files...)
		wal.mu.Unlock()
	}
	return manifest, nil
}

func (s *shard) snapshotManifestPath() string {
	return filepath.Join(s.dataPath, snapshotManifestFileName)
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (s *shard) writeSnapshotManifest(manifest *snapshotCommitManifest) error {
	if manifest == nil || manifest.Version != snapshotManifestVersion ||
		(manifest.State != snapshotManifestPrepared && manifest.State != snapshotManifestCommitted) {
		return errors.New("invalid snapshot commit manifest")
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(s.dataPath, 0o750); err != nil {
		return err
	}
	name := s.snapshotManifestPath()
	tmp := name + ".tmp"
	fd, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = fd.Write(data); err == nil {
		err = fd.Sync()
	}
	closeErr := fd.Close()
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	if err = os.Rename(tmp, name); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncDirectory(filepath.Dir(name))
}

func (s *shard) readSnapshotManifest() (*snapshotCommitManifest, error) {
	data, err := os.ReadFile(s.snapshotManifestPath())
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	manifest := &snapshotCommitManifest{}
	if err = json.Unmarshal(data, manifest); err != nil {
		return nil, err
	}
	if manifest.Version != snapshotManifestVersion ||
		(manifest.State != snapshotManifestPrepared && manifest.State != snapshotManifestCommitted) {
		return nil, errors.New("unsupported snapshot commit manifest")
	}
	return manifest, nil
}

func validateManifestFile(name string, size int64) (bool, error) {
	info, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.IsDir() || info.Size() != size {
		return false, fmt.Errorf("snapshot output %q has size %d, expected %d", name, info.Size(), size)
	}
	return true, nil
}

func rollForwardPreparedFiles(files []snapshotManifestFile) error {
	for i := range files {
		entry := &files[i]
		finalExists, err := validateManifestFile(entry.Final, entry.Size)
		if err != nil {
			return err
		}
		temporaryExists, err := validateManifestFile(entry.Temporary, entry.Size)
		if err != nil {
			return err
		}
		if finalExists && temporaryExists && entry.Final != entry.Temporary {
			return fmt.Errorf("both temporary and final snapshot outputs exist: %q", entry.Final)
		}
		if finalExists {
			continue
		}
		if !temporaryExists {
			return fmt.Errorf("snapshot output is missing: %q", entry.Final)
		}
		if err = os.Rename(entry.Temporary, entry.Final); err != nil {
			return err
		}
		if err = syncDirectory(filepath.Dir(entry.Final)); err != nil {
			return err
		}
	}
	return nil
}

func validateCommittedFiles(files []snapshotManifestFile) error {
	for i := range files {
		exists, err := validateManifestFile(files[i].Final, files[i].Size)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("committed snapshot output is missing: %q", files[i].Final)
		}
	}
	return nil
}

func (s *shard) removeManifestWAL(files []string) error {
	var errs []error
	lockPath := ""
	if s.lock != nil {
		lockPath = *s.lock
	}
	lock := fileops.FileLockOption(lockPath)
	dirs := make(map[string]struct{})
	for _, name := range files {
		if err := fileops.Remove(name, lock); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
			continue
		}
		dirs[filepath.Dir(name)] = struct{}{}
	}
	for dir := range dirs {
		if err := syncDirectory(dir); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *shard) removeSnapshotManifest() error {
	name := s.snapshotManifestPath()
	if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDirectory(filepath.Dir(name))
}

// recoverSnapshotCommit runs before immutable files are loaded and before WAL
// replay. It only rolls a prepared generation forward; it never guesses or
// drops an output whose size/path does not match the durable manifest.
func (s *shard) recoverSnapshotCommit() error {
	manifest, err := s.readSnapshotManifest()
	if err != nil || manifest == nil {
		return err
	}
	if manifest.State == snapshotManifestPrepared {
		if err = rollForwardPreparedFiles(manifest.Files); err != nil {
			return err
		}
		manifest.State = snapshotManifestCommitted
		if err = s.writeSnapshotManifest(manifest); err != nil {
			return err
		}
	} else if err = validateCommittedFiles(manifest.Files); err != nil {
		return err
	}
	return nil
}

// completeSnapshotCommitRecovery runs only after immutable.Open has parsed the
// rolled-forward files successfully. Keeping the committed manifest and WAL
// until that point prevents a size-correct but unreadable output from losing
// its replay source.
func (s *shard) completeSnapshotCommitRecovery() error {
	manifest, err := s.readSnapshotManifest()
	if err != nil || manifest == nil {
		return err
	}
	if manifest.State != snapshotManifestCommitted {
		return errors.New("snapshot recovery is not committed")
	}
	if err = validateCommittedFiles(manifest.Files); err != nil {
		return err
	}
	if err = s.removeManifestWAL(manifest.WAL); err != nil {
		return err
	}
	return s.removeSnapshotManifest()
}
