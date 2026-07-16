// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mutable

import (
	"errors"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	Statistics "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/savsgio/dictpool"
)

type RecordIterator interface {
	Next(dst *record.Record) (uint64, error)
}

type tsMemTableImpl struct {
}

func NewTsMemTableImpl() *tsMemTableImpl {
	return &tsMemTableImpl{}
}

func (t *tsMemTableImpl) WriteRecordForFlush(rec *record.Record, msb *immutable.MsBuilder, tbStore immutable.TablesStore, id uint64) (*immutable.MsBuilder, error) {
	if err := record.ValidateRecord(rec); err != nil {
		return msb, err
	}
	if msb == nil || tbStore == nil {
		return msb, errors.New("nil builder or table store")
	}
	msb.StoreTimes()
	return msb.WriteRecord(id, rec, func(fn immutable.TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16) {
		return tbStore.NextSequence(), 0, 0, 0
	})
}

func (t *tsMemTableImpl) FlushChunks(table *MemTable, dataPath, msName, _, _ string, lock *string, tbStore immutable.TablesStore, _ int64, fileInfos chan []immutable.FileInfoExtend) (_ *FlushResult, retErr error) {
	msInfo, ok := table.msInfoMap[msName]
	if !ok || msInfo == nil {
		return nil, nil
	}
	sids := msInfo.GetAllSid()
	defer PutSidsImpl(sids)

	sidMap := msInfo.sidMap
	sidLen := len(sids)

	hlp := record.NewColumnSortHelper()
	defer hlp.Release()

	var orderMsBuilder, unOrderMsBuilder *immutable.MsBuilder
	defer func() {
		if retErr == nil {
			return
		}
		abortErr := errors.Join(orderMsBuilder.Abort(), unOrderMsBuilder.Abort())
		if abortErr != nil {
			retErr = errors.Join(retErr, &SnapshotAbortError{Err: abortErr})
		}
	}()
	var mmsIdTime *immutable.MmsIdTime
	var flushTime int64 = math.MinInt64
	var orderRowsTotal, unorderRowsTotal, rowsTotal int64
	var oooTimeBins [11]int64

	recPool := []record.Record{{}, {}}
	hasOrderFile := tbStore.GetTableFileNum(msName, true) > 0

	if hasOrderFile {
		seq := tbStore.Sequencer()
		defer func() {
			seq.UnRef()
		}()
		mmsIdTime = seq.GetMmsIdTime(msName)
	}

	for i := range sids {
		chunk := sidMap[sids[i]]
		if err := record.ValidateRecord(chunk.WriteRec.GetRecord()); err != nil {
			return nil, err
		}
		chunk.SortRecord(hlp)
		rec := chunk.WriteRec.GetRecord()

		if hasOrderFile {
			flushTime = math.MaxInt64
			if mmsIdTime != nil {
				flushTime, _ = mmsIdTime.Get(chunk.Sid)
			}
		}

		orderRec, unOrderRec := SplitRecordByTime(rec, recPool, flushTime)
		orderRows := orderRec.RowNums()
		if orderRows > 0 {
			var err error
			if orderMsBuilder == nil {
				conf := immutable.GetTsStoreConfig()
				orderMsBuilder, err = createMsBuilder(tbStore, true, lock, dataPath, msName, sidLen, orderRows, conf, config.TSSTORE)
				if err != nil {
					return nil, err
				}
				orderMsBuilder.DeferSequencerUpdate()
			}
			orderMsBuilder, err = t.WriteRecordForFlush(orderRec, orderMsBuilder, tbStore, chunk.Sid)
			if err != nil {
				return nil, err
			}
			orderRowsTotal += int64(orderRows)
		}

		unOrderRows := unOrderRec.RowNums()
		if unOrderRows > 0 {
			var err error
			if unOrderMsBuilder == nil {
				conf := immutable.GetTsStoreConfig()
				unOrderMsBuilder, err = createMsBuilder(tbStore, false, lock, dataPath, msName, sidLen, unOrderRows, conf, config.TSSTORE)
				if err != nil {
					return nil, err
				}
				unOrderMsBuilder.DeferSequencerUpdate()
			}
			unOrderMsBuilder, err = t.WriteRecordForFlush(unOrderRec, unOrderMsBuilder, tbStore, chunk.Sid)
			if err != nil {
				return nil, err
			}
			unorderRowsTotal += int64(unOrderRows)
			accumulateUnorderedStats(&oooTimeBins, unOrderRec.Times(), flushTime)
		}

		rowsTotal += int64(orderRows + unOrderRows)
	}

	orderFiles, err := t.finish(orderMsBuilder)
	if err != nil {
		return nil, err
	}
	unOrderFiles, err := t.finish(unOrderMsBuilder)
	if err != nil {
		return nil, err
	}

	infos := make([]immutable.FileInfoExtend, 0)
	if orderMsBuilder != nil {
		infos = append(infos, orderMsBuilder.FilesInfo...)
	}
	if unOrderMsBuilder != nil {
		infos = append(infos, unOrderMsBuilder.FilesInfo...)
	}
	return &FlushResult{
		measurement:  msName,
		flushed:      msInfo.GetFlushed(),
		orderFiles:   orderFiles,
		unorderFiles: unOrderFiles,
		fileInfos:    infos,
		fileInfoCh:   fileInfos,
		builders:     []*immutable.MsBuilder{orderMsBuilder, unOrderMsBuilder},
		orderRows:    orderRowsTotal,
		unorderRows:  unorderRowsTotal,
		rows:         rowsTotal,
		oooTimeBins:  oooTimeBins,
	}, nil
}

func (t *tsMemTableImpl) FlushRecords(tbStore immutable.TablesStore, itr RecordIterator, msName, dataPath string,
	lock *string, fileInfos chan []immutable.FileInfoExtend) ([]immutable.TSSPFile, []immutable.TSSPFile, error) {

	hlp := record.NewColumnSortHelper()
	defer hlp.Release()

	var orderMsBuilder, unOrderMsBuilder *immutable.MsBuilder
	abort := func(err error) ([]immutable.TSSPFile, []immutable.TSSPFile, error) {
		return nil, nil, errors.Join(err, orderMsBuilder.Abort(), unOrderMsBuilder.Abort())
	}
	var mmsIdTime *immutable.MmsIdTime
	var flushTime int64 = math.MinInt64

	recPool := []record.Record{{}, {}}
	hasOrderFile := tbStore.GetTableFileNum(msName, true) > 0

	if hasOrderFile {
		seq := tbStore.Sequencer()
		defer func() {
			seq.UnRef()
		}()
		mmsIdTime = seq.GetMmsIdTime(msName)
	}

	rec := &record.Record{}
	for {
		rec.ResetDeep()
		sid, err := itr.Next(rec)
		if err == io.EOF {
			break
		}
		if err != nil {
			return abort(err)
		}
		if sid == 0 {
			return abort(errors.New("invalid series id"))
		}
		if err = record.ValidateRecord(rec); err != nil {
			return abort(err)
		}

		rec = hlp.Sort(rec)

		if hasOrderFile {
			flushTime = math.MaxInt64
			if mmsIdTime != nil {
				flushTime, _ = mmsIdTime.Get(sid)
			}
		}

		orderRec, unOrderRec := SplitRecordByTime(rec, recPool, flushTime)
		orderRows := orderRec.RowNums()
		if orderRows > 0 {
			if orderMsBuilder == nil {
				conf := immutable.GetTsStoreConfig()
				orderMsBuilder, err = createMsBuilder(tbStore, true, lock, dataPath, msName, 0, orderRows, conf, config.TSSTORE)
				if err != nil {
					return abort(err)
				}
			}
			orderMsBuilder, err = t.WriteRecordForFlush(orderRec, orderMsBuilder, tbStore, sid)
			if err != nil {
				return abort(err)
			}
			atomic.AddInt64(&Statistics.PerfStat.FlushOrderRowsCount, int64(orderRows))
		}

		unOrderRows := unOrderRec.RowNums()
		if unOrderRows > 0 {
			if unOrderMsBuilder == nil {
				conf := immutable.GetTsStoreConfig()
				unOrderMsBuilder, err = createMsBuilder(tbStore, false, lock, dataPath, msName, 0, unOrderRows, conf, config.TSSTORE)
				if err != nil {
					return abort(err)
				}
			}
			unOrderMsBuilder, err = t.WriteRecordForFlush(unOrderRec, unOrderMsBuilder, tbStore, sid)
			if err != nil {
				return abort(err)
			}
			atomic.AddInt64(&Statistics.PerfStat.FlushUnOrderRowsCount, int64(unOrderRows))

			t.statUnordered(unOrderRec.Times(), flushTime)
		}

		atomic.AddInt64(&Statistics.PerfStat.FlushRowsCount, int64(orderRows+unOrderRows))
	}

	orderFiles, err := t.finishAndPublish(orderMsBuilder)
	if err != nil {
		return abort(err)
	}
	unOrderFiles, err := t.finishAndPublish(unOrderMsBuilder)
	if err != nil {
		return abort(err)
	}
	if fileInfos != nil {
		infos := make([]immutable.FileInfoExtend, 0)
		if orderMsBuilder != nil {
			infos = append(infos, orderMsBuilder.FilesInfo...)
		}
		if unOrderMsBuilder != nil {
			infos = append(infos, unOrderMsBuilder.FilesInfo...)
		}
		if len(infos) > 0 {
			fileInfos <- infos
		}
	}

	return orderFiles, unOrderFiles, nil
}

func (t *tsMemTableImpl) statUnordered(times []int64, flushTime int64) {
	if flushTime == math.MaxInt64 {
		return
	}

	stat := Statistics.NewOOOTimeDistribution()
	for i := range times {
		stat.Add(flushTime-times[i], 1)
	}
}

var oooTimeBucketLowerBounds = [...]int64{
	0,
	15 * int64(time.Second),
	30 * int64(time.Second),
	60 * int64(time.Second),
	120 * int64(time.Second),
	240 * int64(time.Second),
	480 * int64(time.Second),
	960 * int64(time.Second),
	3600 * int64(time.Second),
	28800 * int64(time.Second),
	86400 * int64(time.Second),
}

func accumulateUnorderedStats(bins *[11]int64, times []int64, flushTime int64) {
	if flushTime == math.MaxInt64 {
		return
	}
	for _, tm := range times {
		delay := flushTime - tm
		idx := len(oooTimeBucketLowerBounds) - 1
		for i := 1; i < len(oooTimeBucketLowerBounds); i++ {
			if delay < oooTimeBucketLowerBounds[i] {
				idx = i - 1
				break
			}
		}
		bins[idx]++
	}
}

func (r *FlushResult) publishStats() {
	if r == nil {
		return
	}
	atomic.AddInt64(&Statistics.PerfStat.FlushOrderRowsCount, r.orderRows)
	atomic.AddInt64(&Statistics.PerfStat.FlushUnOrderRowsCount, r.unorderRows)
	atomic.AddInt64(&Statistics.PerfStat.FlushRowsCount, r.rows)
	stat := Statistics.NewOOOTimeDistribution()
	for i, count := range r.oooTimeBins {
		if count > 0 {
			stat.Add(oooTimeBucketLowerBounds[i], count)
		}
	}
}

func (t *tsMemTableImpl) finish(msb *immutable.MsBuilder) ([]immutable.TSSPFile, error) {
	if msb == nil {
		return nil, nil
	}

	if err := immutable.FinalizeMsBuilder(msb, true); err != nil {
		return nil, err
	}
	return msb.Files, nil
}

// finishAndPublish preserves the non-snapshot FlushRecords contract used by
// Shelf WAL conversion: returned files must already have their final names.
// Snapshot FlushChunks intentionally uses finish instead and publishes only
// after every measurement has prepared successfully.
func (t *tsMemTableImpl) finishAndPublish(msb *immutable.MsBuilder) ([]immutable.TSSPFile, error) {
	if msb == nil {
		return nil, nil
	}
	if err := immutable.WriteIntoFile(msb, true, false, nil); err != nil {
		return nil, err
	}
	return msb.Files, nil
}

func SplitRecordByTime(rec *record.Record, pool []record.Record, time int64) (*record.Record, *record.Record) {
	times := rec.Times()
	if time >= times[len(times)-1] {
		return nil, rec
	} else if time < times[0] {
		return rec, nil
	}

	n := sort.Search(len(times), func(i int) bool {
		return times[i] > time
	})

	if len(pool) < 2 {
		pool = make([]record.Record, 2)
	}

	orderRec := &pool[0]
	orderRec.Reset()
	orderRec.ReserveColVal(len(rec.Schema))

	unOrderRec := &pool[1]
	unOrderRec.Reset()
	unOrderRec.ReserveColVal(len(rec.Schema))

	orderIdx, unOrderIdx := 0, 0

	for i := range rec.Schema {
		field := rec.Schema[i]
		col := &rec.ColVals[i]

		orderCol, unOrderCol := &orderRec.ColVals[orderIdx], &unOrderRec.ColVals[unOrderIdx]

		unOrderCol.Init()
		unOrderCol.AppendColVal(col, field.Type, 0, n)
		if unOrderCol.NilCount != unOrderCol.Len {
			unOrderRec.Schema = append(unOrderRec.Schema, field)
			unOrderIdx++
		}

		orderCol.Init()
		orderCol.AppendColVal(col, field.Type, n, col.Len)
		if orderCol.NilCount != orderCol.Len {
			orderRec.Schema = append(orderRec.Schema, field)
			orderIdx++
		}
	}

	orderRec.ColVals = orderRec.ColVals[:orderIdx]
	unOrderRec.ColVals = unOrderRec.ColVals[:unOrderIdx]

	return orderRec, unOrderRec
}

func (t *tsMemTableImpl) WriteRows(table *MemTable, rowsD *dictpool.Dict, wc WriteRowsCtx) error {
	table.batchWriteMu.Lock()
	defer table.batchWriteMu.Unlock()

	if err := t.preflightVarBytesBatch(table, rowsD); err != nil {
		return err
	}

	var err error
	for _, mapp := range rowsD.D {
		rows, ok := mapp.Value.(*[]influx.Row)
		if !ok {
			return errors.New("can't map mmPoints")
		}
		rs := *rows
		msName := stringinterner.InternSafe(mapp.Key)

		start := time.Now()
		msInfo := table.CreateMsInfo(msName, &rs[0], nil)
		atomic.AddInt64(&Statistics.PerfStat.WriteGetMstInfoNs, time.Since(start).Nanoseconds())

		start = time.Now()
		var (
			exist bool
			sid   uint64
			chunk *WriteChunk
		)
		for index := range rs {
			sid = rs[index].PrimaryId
			if sid == 0 {
				continue
			}

			chunk, exist = msInfo.CreateChunk(sid)

			if !exist && table.idx != nil {
				// only range mode
				startTime := time.Now()
				err = table.idx.CreateIndex(util.Str2bytes(msName), rs[index].ShardKey, sid)
				if err != nil {
					return err
				}
				atomic.AddInt64(&Statistics.PerfStat.WriteShardKeyIdxNs, time.Since(startTime).Nanoseconds())
			}
			_, err = t.appendFields(msInfo, chunk, rs[index].Timestamp, rs[index].Fields)
			if err != nil {
				return err
			}

			wc.AddRowCountsBySid(msName, sid, 1)
		}

		atomic.AddInt64(&Statistics.PerfStat.WriteMstInfoNs, time.Since(start).Nanoseconds())
	}

	return nil
}

type varBytesBatchKey struct {
	measurement string
	sid         uint64
	field       string
}

func (t *tsMemTableImpl) preflightVarBytesBatch(table *MemTable, rowsD *dictpool.Dict) error {
	limit := record.GetMaxVarColValBytes()
	deltas := make(map[varBytesBatchKey]int64)

	for _, mapp := range rowsD.D {
		rows, ok := mapp.Value.(*[]influx.Row)
		if !ok {
			return errors.New("can't map mmPoints")
		}
		if len(*rows) == 0 {
			return errors.New("empty measurement row batch")
		}
		for i := range *rows {
			row := &(*rows)[i]
			if row.PrimaryId == 0 {
				continue
			}
			for j := range row.Fields {
				field := &row.Fields[j]
				if field.Type != influx.Field_Type_String && field.Type != influx.Field_Type_Tag {
					continue
				}
				valueBytes := int64(len(field.StrValue))
				if err := record.CanAppendVarBytes(0, valueBytes, limit); err != nil {
					return err
				}
				key := varBytesBatchKey{measurement: mapp.Key, sid: row.PrimaryId, field: field.Key}
				delta := deltas[key]
				if valueBytes > limit-delta {
					return errno.NewError(errno.ErrValueTooLarge, delta+valueBytes, limit)
				}
				deltas[key] = delta + valueBytes
			}
		}
	}

	for key, delta := range deltas {
		current, err := currentVarBytes(table, key)
		if err != nil {
			return err
		}
		if err = record.CanAppendVarBytes(current, delta, limit); err != nil {
			return err
		}
	}
	return nil
}

func currentVarBytes(table *MemTable, key varBytesBatchKey) (int64, error) {
	table.mu.RLock()
	msInfo := table.msInfoMap[key.measurement]
	table.mu.RUnlock()
	if msInfo == nil {
		return 0, nil
	}

	msInfo.mu.RLock()
	chunk := msInfo.sidMap[key.sid]
	msInfo.mu.RUnlock()
	if chunk == nil {
		return 0, nil
	}

	chunk.Mu.Lock()
	defer chunk.Mu.Unlock()
	rec := chunk.WriteRec.rec
	if rec == nil {
		return 0, nil
	}
	idx := rec.Schema.FieldIndex(key.field)
	if idx < 0 {
		return 0, nil
	}
	if rec.Schema[idx].Type != influx.Field_Type_String && rec.Schema[idx].Type != influx.Field_Type_Tag {
		return 0, errno.NewError(errno.ErrCorruptColumn, "string field conflicts with existing non-string column")
	}
	if err := record.ValidateCol(&rec.ColVals[idx], rec.Schema[idx].Type); err != nil {
		return 0, err
	}
	return rec.ColVals[idx].VarBytes(), nil
}

func (t *tsMemTableImpl) appendFields(msInfo *MsInfo, chunk *WriteChunk, time int64, fields []influx.Field) (int64, error) {
	chunk.Mu.Lock()
	defer chunk.Mu.Unlock()

	writeRec := &chunk.WriteRec
	if writeRec.rec == nil {
		writeRec.init(msInfo.Schema)
	}

	sameSchema := checkSchemaIsSame(writeRec.rec.Schema, fields)
	if !sameSchema && !writeRec.schemaCopyed {
		copySchema := record.Schemas{}
		if writeRec.rec.RowNums() == 0 {
			genMsSchema(&copySchema, fields)
			sameSchema = true
		} else {
			copySchema = append(copySchema, writeRec.rec.Schema...)
		}
		oldColNums := writeRec.rec.ColNums()
		newColNums := len(copySchema)
		writeRec.rec.Schema = copySchema
		writeRec.rec.ReserveColVal(newColNums - oldColNums)
		writeRec.schemaCopyed = true
	}

	if time <= writeRec.lastAppendTime {
		writeRec.timeAsd = false
	} else {
		writeRec.lastAppendTime = time
	}
	if time < writeRec.firstAppendTime {
		writeRec.firstAppendTime = time
	}

	return record.AppendFieldsToRecord(writeRec.rec, fields, time, sameSchema)
}

func (t *tsMemTableImpl) WriteCols(table *MemTable, rec *record.Record, mst string) error {
	return nil
}

func (t *tsMemTableImpl) Reset(table *MemTable) {

}

func (t *tsMemTableImpl) initMsInfo(msInfo *MsInfo, row *influx.Row, rec *record.Record, name string) *MsInfo {
	msInfo.Init(row)
	return msInfo
}

func (t *tsMemTableImpl) SetFlushManagerInfo(manager map[string]FlushManager, accumulateMetaIndex *sync.Map) {
}
