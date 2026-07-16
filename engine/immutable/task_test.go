// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/scheduler"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestFullCompactOneFile(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 10)()
	schemas := getDefaultSchemas()

	immutable.SetMergeFlag4TsStore(1)
	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, false)

	conf := config.GetStoreConfig()
	conf.ParquetTask.TSSPToParquetLevel = 2
	conf.Compact.CompactRecovery = true
	defer func() {
		conf.ParquetTask.TSSPToParquetLevel = 0
		conf.Compact.CompactRecovery = false
	}()

	mIndex := &immutable.MockIndexMergeSet{
		GetSeriesFn: func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
			return nil
		},
		SearchSeriesKeysFn: func(series [][]byte, name []byte, condition influxql.Expr) ([][]byte, error) {
			return [][]byte{}, nil
		},
	}
	mh.store.SetIndexMergeSet(mIndex)

	for i := 0; i < 5; i++ {
		rg.setBegin(begin).incrBegin(i + 5)
		mh.addRecord(100, rg.generate(schemas, 1))
		require.NoError(t, mh.saveToOrder())

		require.NoError(t, mh.store.FullCompact(1))
		mh.store.Wait()
	}

	require.NoError(t, mh.store.FullCompact(1))
	mh.store.Wait()
	require.Equal(t, 1, len(mh.store.Order["mst"].Files()))
}

func TestUint32NonstreamRetry_RebuildsPlanAndRunsStream(t *testing.T) {
	defer beforeTest(t, 10)()
	oldLimit := record.GetMaxVarColValBytes()
	require.NoError(t, record.SetMaxVarColValBytes(10))
	t.Cleanup(func() { require.NoError(t, record.SetMaxVarColValBytes(oldLimit)) })

	oldMode := immutable.GetMergeFlag4TsStore()
	immutable.SetMergeFlag4TsStore(util.NonStreamingCompact)
	t.Cleanup(func() { immutable.SetMergeFlag4TsStore(oldMode) })

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	schema := record.Schemas{
		{Name: "value", Type: influx.Field_Type_String},
		{Name: record.TimeField, Type: influx.Field_Type_Int},
	}
	makeRecord := func(value string, ts int64) *record.Record {
		rec := record.NewRecordBuilder(schema)
		rec.ColVals[0].AppendString(value)
		rec.ColVals[1].AppendInteger(ts)
		return rec
	}

	mh.addRecord(100, makeRecord("123456", 1))
	require.NoError(t, mh.saveToOrder())
	mh.addRecord(100, makeRecord("abcdef", 2))
	require.NoError(t, mh.saveToOrder())

	require.NoError(t, mh.store.FullCompact(1))
	mh.store.Wait()
	require.Len(t, mh.store.Order["mst"].Files(), 1)
}

func TestMergeWithParquetTask(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 10)()
	schemas := getDefaultSchemas()

	immutable.SetMergeFlag4TsStore(1)
	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, false)

	conf := config.GetStoreConfig()
	conf.ParquetTask.TSSPToParquetLevel = 2
	conf.Merge.MergeSelfOnly = true
	conf.Merge.MinInterval = 0
	conf.Merge.MaxMergeSelfLevel = 4
	defer func() {
		conf.Merge.MinInterval = toml.Duration(300 * time.Second)
		conf.ParquetTask.TSSPToParquetLevel = 0
		conf.Merge.MaxMergeSelfLevel = 0
		conf.Merge.MergeSelfOnly = false
	}()

	rg.setBegin(begin)
	mh.addRecord(100, rg.generate(schemas, 1))
	require.NoError(t, mh.saveToOrder())

	for i := 0; i < 100; i++ {
		rg.setBegin(begin).incrBegin(i + 5)
		mh.addRecord(100, rg.generate(schemas, 1))
		require.NoError(t, mh.saveToUnordered())
	}

	require.NoError(t, mh.mergeAndCompact(false))
	mh.store.Wait()
	require.NoError(t, mh.mergeAndCompact(false))
	mh.store.Wait()
}

func TestReliabilityLog(t *testing.T) {
	plan := &immutable.TSSP2ParquetPlan{
		Mst: "foo",
		Schema: map[string]uint8{
			"int1": 1, "float1": 2, "string1": 3,
		},
		Files: []string{"1", "2", "3"},
	}
	lockFile := ""

	dir := t.TempDir()
	logFile, err := immutable.SaveReliabilityLog(plan, dir, lockFile, func() string {
		return "plan.log"
	})
	require.NoError(t, err)
	require.Contains(t, logFile, "plan.log")

	other := &immutable.TSSP2ParquetPlan{}
	require.NoError(t, immutable.ReadReliabilityLog(logFile, other))
	require.Equal(t, plan, other)

	signal := make(chan struct{})
	defer func() {
		close(signal)
	}()
	lm := limiter.NewFixed(1)
	sch := scheduler.NewTaskScheduler(func(signal chan struct{}, onClose func()) {}, lm)

	ctx := immutable.NewEventContext(&immutable.MockIndexMergeSet{GetSeriesFn: func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
		return nil
	}}, sch, signal)

	require.NotEmpty(t, immutable.ReadReliabilityLog(logFile+".not_exists", other))
	require.NoError(t, immutable.ProcParquetLog(dir, &lockFile, ctx))

	fp, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	require.NoError(t, err)
	_, err = fp.Write(make([]byte, 10))
	require.NoError(t, err)
	require.NoError(t, fp.Sync())
	require.NoError(t, fp.Close())

	require.NotEmpty(t, immutable.ReadReliabilityLog(dir, other))
	require.NoError(t, immutable.ProcParquetLog(dir, &lockFile, ctx))
}

func assertDirFileNumber(t *testing.T, dir string, exp int) {
	items, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Equal(t, exp, len(items))
}

func TestTSSP2ParquetEvent(t *testing.T) {
	config.GetStoreConfig().ParquetTask.TSSPToParquetLevel = 2
	defer func() {
		config.GetStoreConfig().ParquetTask.TSSPToParquetLevel = 0
	}()

	shardDir := filepath.Join(t.TempDir(), "shard")
	logDir := filepath.Join(shardDir, "parquet_log")
	lock := ""
	var begin int64 = 1e12
	rg := newRecordGenerator(begin, defaultInterval, false)
	rec := rg.generate(getDefaultSchemas(), 1)

	var createEvent = func() immutable.Event {
		event := &immutable.MergeSelfParquetEvent{}
		event.Init("mst", 2)
		require.True(t, event.Enable())

		event.OnWriteRecord(rec)
		return event
	}

	signal := make(chan struct{})
	defer func() {
		close(signal)
	}()
	lm := limiter.NewFixed(1)
	sch := scheduler.NewTaskScheduler(func(signal chan struct{}, onClose func()) {}, lm)

	event := createEvent()
	ctx := immutable.NewEventContext(&immutable.MockIndexMergeSet{GetSeriesFn: func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
		return nil
	}}, sch, signal)

	require.NoError(t, event.OnReplaceFile(shardDir, lock))
	assertDirFileNumber(t, logDir, 1)
	event.OnFinish(ctx)
	sch.Wait()
	assertDirFileNumber(t, logDir, 0)

	// interrupted before OnReplaceFile
	event = createEvent()
	event.OnInterrupt()
	require.NoError(t, event.OnReplaceFile(shardDir, lock))
	assertDirFileNumber(t, logDir, 0)
	event.OnFinish(ctx)
	sch.Wait()

	// interrupted before OnFinish
	event = createEvent()
	require.NoError(t, event.OnReplaceFile(shardDir, lock))
	assertDirFileNumber(t, logDir, 1)
	event.OnInterrupt()
	assertDirFileNumber(t, logDir, 0)
	event.OnFinish(ctx)
	sch.Wait()
}

func TestMergeWithParquetTask_full(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()

	conf := config.GetStoreConfig()
	conf.Merge.MergeSelfOnly = true
	conf.ParquetTask.TSSPToParquetLevel = 2
	defer func() {
		conf.ParquetTask.TSSPToParquetLevel = 0
	}()

	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	mh.addRecord(uint64(100), rg.generate(schemas, 10))
	require.NoError(t, mh.saveToOrder())

	for i := 0; i < 103; i++ {
		schemas = getDefaultSchemas()
		mh.addRecord(uint64(100+i%2), rg.incrBegin(1).generate(schemas, 5))
		require.NoError(t, mh.saveToUnordered())
	}

	var assertFileNumber = func(exp int) {
		files, ok := mh.store.OutOfOrder["mst"]
		require.True(t, ok)
		require.Equal(t, exp, files.Len())
	}

	require.NoError(t, mh.mergeAndCompact(false))
	assertFileNumber(19)

	require.NoError(t, mh.mergeAndCompact(false))
	assertFileNumber(12)

	require.NoError(t, mh.store.MergeOutOfOrder(1, true, false))
	mh.store.Wait()
	assertFileNumber(2)

	require.NoError(t, mh.store.MergeOutOfOrder(1, true, false))
	mh.store.Wait()
	assertFileNumber(1)
}
