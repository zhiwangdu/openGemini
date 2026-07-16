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
	"fmt"
	"math"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
)

// RemoveTSSPFileOnAbort converts legacy panic-based close/remove failures into
// an owned attempt error. Callers must fail closed when this returns an error.
func RemoveTSSPFileOnAbort(file TSSPFile) (err error) {
	if file == nil {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("remove temporary tssp file: %v", recovered)
		}
	}()
	return file.Remove()
}

func checkedSegmentSize(actualSize int64) (uint32, error) {
	return record.CheckedUint32Size(actualSize)
}

func setChunkMetaSize(cm *ChunkMeta, actualSize int64) error {
	if cm == nil || actualSize < 0 {
		return errno.NewError(errno.ErrCorruptTSSP, fmt.Sprintf("invalid chunk size %d", actualSize))
	}
	cm.size = uint32(actualSize)
	return nil
}

func nextChunkDataOffset(chunkStart, writerPosition int64, cm *ChunkMeta) (int64, error) {
	if cm == nil || chunkStart < 0 || writerPosition < chunkStart {
		return 0, errno.NewError(errno.ErrCorruptTSSP,
			fmt.Sprintf("invalid chunk range [%d,%d)", chunkStart, writerPosition))
	}
	actualSize := writerPosition - chunkStart
	if uint32(actualSize) != cm.size {
		return 0, errno.NewError(errno.ErrCorruptTSSP,
			fmt.Sprintf("chunk size low bits mismatch: actual=%d stored=%d", actualSize, cm.size))
	}
	return writerPosition, nil
}

func checkedSegmentEnd(offset int64, size uint32) (int64, error) {
	if offset < 0 || offset > math.MaxInt64-int64(size) {
		return 0, errno.NewError(errno.ErrCorruptTSSP,
			fmt.Sprintf("segment range overflows: offset=%d size=%d", offset, size))
	}
	return offset + int64(size), nil
}

func checkedAddInt64(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, errno.NewError(errno.ErrCorruptTSSP,
			fmt.Sprintf("int64 offset addition overflows: %d + %d", a, b))
	}
	return a + b, nil
}

// ChunkEntryRange returns the range covered by the raw entries in one ChunkMeta.
// It does not prove that the raw absolute offsets belong to the expected SID.
func ChunkEntryRange(cm *ChunkMeta, fileDataStart, fileDataEnd int64) (int64, int64, error) {
	if cm == nil || fileDataStart < 0 || fileDataEnd < fileDataStart || cm.offset < fileDataStart || cm.offset > fileDataEnd {
		return 0, 0, errno.NewError(errno.ErrCorruptTSSP, "invalid chunk or file data range")
	}

	rangeEnd := cm.offset
	entryCount := 0
	for i := range cm.colMeta {
		for j := range cm.colMeta[i].entries {
			entry := &cm.colMeta[i].entries[j]
			if entry.offset < cm.offset {
				return 0, 0, errno.NewError(errno.ErrCorruptTSSP,
					fmt.Sprintf("segment starts before chunk: chunk=%d segment=%d", cm.offset, entry.offset))
			}
			end, err := checkedSegmentEnd(entry.offset, entry.size)
			if err != nil {
				return 0, 0, err
			}
			if end > fileDataEnd {
				return 0, 0, errno.NewError(errno.ErrCorruptTSSP,
					fmt.Sprintf("segment ends outside data range: end=%d dataEnd=%d", end, fileDataEnd))
			}
			if end > rangeEnd {
				rangeEnd = end
			}
			entryCount++
		}
	}
	if entryCount == 0 {
		return 0, 0, errno.NewError(errno.ErrCorruptTSSP, "chunk has no segment entries")
	}
	rangeSize := rangeEnd - cm.offset
	if uint32(rangeSize) != cm.size {
		return 0, 0, errno.NewError(errno.ErrCorruptTSSP,
			fmt.Sprintf("chunk entry range low bits mismatch: range=%d stored=%d", rangeSize, cm.size))
	}
	return cm.offset, rangeEnd, nil
}

func segmentWithinRange(seg *Segment, start, end int64) (bool, error) {
	if seg == nil || seg.offset < start {
		return false, nil
	}
	segEnd, err := checkedSegmentEnd(seg.offset, seg.size)
	if err != nil {
		return false, err
	}
	return segEnd <= end, nil
}

func canPreloadSegmentRecord(cm *ChunkMeta, segment int, schema record.Schemas) (bool, error) {
	if cm == nil || segment < 0 || cm.size >= defaultIoSize {
		return false, nil
	}
	candidateEnd, err := checkedSegmentEnd(cm.offset, cm.size)
	if err != nil {
		return false, err
	}
	if len(cm.colMeta) == 0 {
		return false, errno.NewError(errno.ErrCorruptTSSP, "chunk has no columns")
	}

	check := func(seg *Segment) (bool, error) {
		return segmentWithinRange(seg, cm.offset, candidateEnd)
	}
	for i := range schema {
		if schema[i].Name == record.TimeField {
			continue
		}
		idx := cm.columnIndex(&schema[i])
		if idx < 0 {
			continue
		}
		if segment >= len(cm.colMeta[idx].entries) {
			return false, errno.NewError(errno.ErrCorruptTSSP, "field segment index out of range")
		}
		inside, rangeErr := check(&cm.colMeta[idx].entries[segment])
		if rangeErr != nil || !inside {
			return false, rangeErr
		}
	}

	timeMeta := cm.timeMeta()
	if segment >= len(timeMeta.entries) {
		return false, errno.NewError(errno.ErrCorruptTSSP, "time segment index out of range")
	}
	return check(&timeMeta.entries[segment])
}

func cloneChunkMetaForCopy(source *ChunkMeta, rangeStart, rangeEnd, targetStart int64) (*ChunkMeta, error) {
	if source == nil || rangeStart != source.offset || rangeEnd < rangeStart || targetStart < 0 {
		return nil, errno.NewError(errno.ErrCorruptTSSP, "invalid chunk copy range")
	}
	target := source.Clone()
	delta := targetStart - rangeStart
	target.offset = targetStart
	for i := range target.colMeta {
		for j := range target.colMeta[i].entries {
			offset, err := checkedAddInt64(target.colMeta[i].entries[j].offset, delta)
			if err != nil || offset < targetStart {
				if err != nil {
					return nil, err
				}
				return nil, errno.NewError(errno.ErrCorruptTSSP, "copied segment starts before target chunk")
			}
			target.colMeta[i].entries[j].offset = offset
		}
	}
	if err := setChunkMetaSize(target, rangeEnd-rangeStart); err != nil {
		return nil, err
	}
	return target, nil
}

func nextChunkCopySize(copied, total, limit int64) (uint32, error) {
	if copied < 0 || total < 0 || copied > total || limit <= 0 {
		return 0, errno.NewError(errno.ErrCorruptTSSP, "invalid chunk copy progress")
	}
	remaining := total - copied
	if remaining == 0 {
		return 0, nil
	}
	if remaining > limit {
		remaining = limit
	}
	if remaining > math.MaxUint32 {
		remaining = math.MaxUint32
	}
	return uint32(remaining), nil
}
