// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package engine

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/clv"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var tsmCursorPool = &sync.Pool{}

// lazyUnorderedMergeEnabled gates the lazy out-of-order merge path (Phase 3 of
// tsmMergeCursor_FirstTimeInit_unordered_analysis.md). Default off: the eager FirstTimeInit
// full-read path is used. Enable to defer reading out-of-order data until the ordered
// watermark advances, reducing first-packet latency and the chain-merge cost.
//
// It is an atomic because the setter may be called from a config/admin goroutine while query
// goroutines read it in lazyUnorderedEnabled; prefer setting it once at startup.
var lazyUnorderedMergeEnabled atomic.Bool

// SetLazyUnorderedMergeEnabled toggles the lazy out-of-order merge path at runtime.
func SetLazyUnorderedMergeEnabled(en bool) { lazyUnorderedMergeEnabled.Store(en) }

// GetLazyUnorderedMergeEnabled reports whether the lazy out-of-order merge path is enabled.
func GetLazyUnorderedMergeEnabled() bool { return lazyUnorderedMergeEnabled.Load() }

func getTsmCursor() *tsmMergeCursor {
	v := tsmCursorPool.Get()
	if v != nil {
		return v.(*tsmMergeCursor)
	}
	cursor := &tsmMergeCursor{}
	return cursor
}
func putTsmCursor(cursor *tsmMergeCursor) {
	tsmCursorPool.Put(cursor)
}

type tsmMergeCursor struct {
	ctx *idKeyCursorContext

	sid                 uint64
	span                *tracing.Span
	ops                 []*comm.CallOption
	locations           *immutable.LocationCursor
	outOfOrderLocations *immutable.LocationCursor
	locationInit        bool
	onlyFirstOrLast     bool

	filter     influxql.Expr
	rowFilters *[]clv.RowFilter
	tags       *influx.PointTags

	orderRecIter    recordIter
	outOrderRecIter recordIter
	recordPool      *record.CircularRecordPool
	// unorderPool reuses the per-iteration dst builder in the non-aggregate FirstTimeInit
	// out-of-order read loop, avoiding a NewRecordBuilder allocation on every iteration.
	// See unorderRecordNum for the ring size and the merge-scratch exclusion rationale.
	unorderPool    *record.CircularRecordPool
	limitFirstTime int64
	init           bool
	lazyInit       bool
	// lazyMerger, when non-nil, drives the lazy out-of-order merge path: out-of-order data is
	// read and merged in watermark-bounded batches instead of all up front in FirstTimeInit.
	lazyMerger *lazyUnorderedMerger
}

// lazyUnorderedMergeMinLocations is the minimum number of matched out-of-order locations for the
// lazy path to be worth using. Below this, the eager chain merge is cheaper (its O(N^2*R) cost
// is small for small N) and the lazy heap/merge overhead would regress total query time. The
// threshold is conservative so the lazy path never regresses the small-N common case; it wins
// on both time and memory once out-of-order files are numerous (the scenario the optimization
// targets). 0 disables the threshold (always use lazy when the flag is on) - intended for tests.
// See BenchmarkTotal_NoOverlap for the crossover.
var lazyUnorderedMergeMinLocations int32 = 64

// SetLazyUnorderedMergeMinLocations tunes the minimum out-of-order location count for the lazy
// path. Intended to be set once at startup.
func SetLazyUnorderedMergeMinLocations(n int32) {
	atomic.StoreInt32(&lazyUnorderedMergeMinLocations, n)
}

// lazyUnorderedEnabled reports whether the lazy out-of-order merge path should be used for the
// current cursor. It is restricted to ascending non-aggregate reads without limit-cut or Prom
// semantics; every other shape falls back to the eager path.
func (c *tsmMergeCursor) lazyUnorderedEnabled() bool {
	if !lazyUnorderedMergeEnabled.Load() {
		return false
	}
	if len(c.ops) > 0 {
		return false
	}
	if c.ctx.querySchema.CanLimitCut() {
		return false
	}
	opt := c.ctx.querySchema.Options()
	if opt.IsPromQuery() || opt.IsPromRemoteRead() {
		return false
	}
	// Only worth the lazy heap merge when there are enough out-of-order locations; otherwise the
	// eager path is faster and the memory saving is negligible.
	if min := atomic.LoadInt32(&lazyUnorderedMergeMinLocations); min > 0 && int32(c.outOfOrderLocations.Len()) < min {
		return false
	}
	return true
}

// newFilterOpts builds the FilterOptions used by both the eager and lazy read paths.
func (c *tsmMergeCursor) newFilterOpts() *immutable.FilterOptions {
	return immutable.NewFilterOpts(c.filter, &c.ctx.filterOption, c.tags, c.rowFilters)
}

func newTsmMergeCursor(ctx *idKeyCursorContext, sid uint64, filter influxql.Expr, rowFilters *[]clv.RowFilter,
	tags *influx.PointTags, lazyInit bool, _ *tracing.Span) (*tsmMergeCursor, error) {

	c := getTsmCursor()
	c.ctx = ctx
	c.lazyInit = lazyInit
	c.filter = filter
	c.rowFilters = rowFilters
	c.tags = tags
	c.orderRecIter.reset()
	c.outOrderRecIter.reset()
	c.sid = sid
	// if not sortedSeries, add location info in next function.
	if !lazyInit {
		c.locations = immutable.NewLocationCursor(len(ctx.readers.Orders))
		c.outOfOrderLocations = immutable.NewLocationCursor(len(ctx.readers.OutOfOrders))
		if e := c.AddLoc(); e != nil {
			return nil, e
		}
		if c.locations.Len() == 0 && c.outOfOrderLocations.Len() == 0 {
			return nil, nil
		}
	} else {
		c.locations = nil
		c.outOfOrderLocations = nil
	}
	return c, nil
}

func newTsmMergeCursorWithShard(ctx *idKeyCursorContext, shardId, sid uint64, filter influxql.Expr, rowFilters *[]clv.RowFilter,
	tags *influx.PointTags, _ *tracing.Span) (*tsmMergeCursor, error) {
	c := getTsmCursor()
	c.ctx = ctx
	c.filter = filter
	c.rowFilters = rowFilters
	c.tags = tags
	c.orderRecIter.reset()
	c.outOrderRecIter.reset()
	c.sid = sid
	if err := c.AddLocWithShard(shardId, sid); err != nil {
		return nil, err
	}
	if c.locations.Len() == 0 && c.outOfOrderLocations.Len() == 0 {
		return nil, nil
	}
	return c, nil
}

func AddLocations(l *immutable.LocationCursor, files immutable.TableReaders, ctx *idKeyCursorContext, sid uint64, metaCtx *immutable.ChunkMetaContext) error {
	for _, r := range files {
		if ctx.IsAborted() {
			return nil
		}
		loc := immutable.NewLocation(r, ctx.decs)
		contains, err := loc.Contains(sid, ctx.tr, metaCtx)
		if err != nil {
			return err
		}
		if contains {
			l.AddLocation(loc)
		}
	}
	return nil
}

func AddLocationsWithInit(l *immutable.LocationCursor, files immutable.TableReaders, ctx *idKeyCursorContext, sid uint64) error {
	var chunkMetaContext *immutable.ChunkMetaContext
	if ctx.querySchema.Options().IsPromQuery() && ctx.metaContext != nil {
		chunkMetaContext = ctx.metaContext
	} else {
		chunkMetaContext = immutable.NewChunkMetaContext(ctx.schema)
		defer chunkMetaContext.Release()
	}
	if err := AddLocations(l, files, ctx, sid, chunkMetaContext); err != nil {
		return err
	}
	return nil
}

func AddLocationsWithLimit(l *immutable.LocationCursor, files immutable.TableReaders, ctx *idKeyCursorContext, sid uint64) (int64, error) {
	var orderRow, firstTime int64
	init := false
	var filesIndex int

	if len(files) == 0 {
		return -1, nil
	}

	option := ctx.querySchema.Options()
	schema := ctx.schema

	chunkMetaContext := immutable.NewChunkMetaContext(schema)
	defer chunkMetaContext.Release()

	for i := range files {
		if !option.IsAscending() {
			filesIndex = len(files) - i - 1
		} else {
			filesIndex = i
		}

		r := files[filesIndex]
		loc := immutable.NewLocation(r, ctx.decs)
		contains, err := loc.Contains(sid, ctx.tr, chunkMetaContext)
		if err != nil {
			return 0, err
		}
		if contains {
			metaMinTime, metaMaxTime := loc.GetChunkMeta().MinMaxTime()

			if !init {
				if option.IsAscending() {
					firstTime = metaMinTime
				} else {
					firstTime = metaMaxTime
				}
				init = true
			}

			row, err := loc.GetChunkMeta().TimeMeta().RowCount(schema.Field(schema.Len()-1), ctx.decs)

			if err != nil {
				return 0, err
			}
			if ctx.tr.Contains(metaMinTime, metaMaxTime) {
				orderRow += row
			}

			l.AddLocation(loc)
			if orderRow >= int64(option.GetLimit()+option.GetOffset()) {
				break
			}
		}
	}
	return firstTime, nil
}

func AddLocationsWithFirstTime(l *immutable.LocationCursor, files immutable.TableReaders, ctx *idKeyCursorContext, sid uint64) (int64, error) {
	ascending := ctx.querySchema.Options().IsAscending()
	var firstTime int64
	firstTime = -1

	chunkMetaContext := immutable.NewChunkMetaContext(ctx.schema)
	defer chunkMetaContext.Release()

	for _, r := range files {
		loc := immutable.NewLocation(r, ctx.decs)
		contains, err := loc.Contains(sid, ctx.tr, chunkMetaContext)
		if err != nil {
			return -1, err
		}
		if contains {
			l.AddLocation(loc)
			metaMinTime, metaMaxTime := loc.GetChunkMeta().MinMaxTime()
			if ascending {
				firstTime = getFirstTime(metaMinTime, firstTime, ascending)
			} else {
				firstTime = getFirstTime(metaMaxTime, firstTime, ascending)
			}
		}
	}
	return firstTime, nil
}

func (c *tsmMergeCursor) ReInit(
	sid uint64,
	filter influxql.Expr,
	rowFilters *[]clv.RowFilter,
	tags *influx.PointTags,
) (bool, error) {
	c.init = true
	c.filter = filter
	c.rowFilters = rowFilters
	c.tags = tags
	c.sid = sid
	c.orderRecIter.reset()
	c.outOrderRecIter.reset()
	c.locations.Reset()
	c.outOfOrderLocations.Reset()
	// Reusing the cursor for a new series: the new series must re-initialize its own out-of-order
	// data. Reset locationInit (so FirstTimeInit re-runs) and drop any stale lazyMerger, whose
	// sources still reference the previous series' (now orphaned) locations.
	c.locationInit = false
	c.lazyMerger = nil
	if err := c.AddLoc(); err != nil {
		return false, err
	}
	if c.locations.Len() == 0 && c.outOfOrderLocations.Len() == 0 {
		return false, nil
	}
	return true, nil
}

func (c *tsmMergeCursor) ReInitWithShard(
	shardId uint64,
	sid uint64,
	filter influxql.Expr,
	rowFilters *[]clv.RowFilter,
	tags *influx.PointTags,
	crossShard bool,
) (bool, error) {
	c.init = true
	c.filter = filter
	c.rowFilters = rowFilters
	c.tags = tags
	c.sid = sid
	c.orderRecIter.reset()
	c.outOrderRecIter.reset()
	c.locations.Reset()
	c.outOfOrderLocations.Reset()
	// See ReInit: re-initialize out-of-order state for the new series.
	c.locationInit = false
	c.lazyMerger = nil
	if !crossShard {
		if err := c.AddLoc(); err != nil {
			return false, err
		}
	} else {
		if err := c.AddLocWithShard(shardId, sid); err != nil {
			return false, err
		}
	}
	if c.locations.Len() == 0 && c.outOfOrderLocations.Len() == 0 {
		return false, nil
	}
	return true, nil
}

func (c *tsmMergeCursor) AddLoc() error {
	var err error
	var limitFirstTime int64
	if c.ctx.querySchema.CanLimitCut() {
		var orderFirstTime, unorderFirstTime int64
		orderFirstTime, err = AddLocationsWithLimit(c.locations, c.ctx.readers.Orders, c.ctx, c.sid)
		if err != nil {
			return err
		}
		unorderFirstTime, err = AddLocationsWithFirstTime(c.outOfOrderLocations, c.ctx.readers.OutOfOrders, c.ctx, c.sid)
		if err != nil {
			return err
		}

		limitFirstTime = getFirstTime(orderFirstTime, unorderFirstTime, c.ctx.querySchema.Options().IsAscending())
	} else {
		err = AddLocationsWithInit(c.locations, c.ctx.readers.Orders, c.ctx, c.sid)
		if err != nil {
			return err
		}
		err = AddLocationsWithInit(c.outOfOrderLocations, c.ctx.readers.OutOfOrders, c.ctx, c.sid)
		if err != nil {
			return err
		}
	}
	c.limitFirstTime = limitFirstTime
	return nil
}

func (c *tsmMergeCursor) AddLocWithShard(shardId uint64, sid uint64) error {
	var err error
	immTable, ok := c.ctx.immTableReaders[shardId]
	if !ok {
		return nil
	}
	c.locations = immutable.NewLocationCursor(len(immTable.Orders))
	c.outOfOrderLocations = immutable.NewLocationCursor(len(immTable.OutOfOrders))
	err = AddLocationsWithInit(c.locations, immTable.Orders, c.ctx, sid)
	if err != nil {
		return err
	}
	err = AddLocationsWithInit(c.outOfOrderLocations, immTable.OutOfOrders, c.ctx, sid)
	if err != nil {
		return err
	}
	return nil
}

func (c *tsmMergeCursor) readData(orderLoc bool, dst *record.Record) (*record.Record, error) {
	c.ctx.decs.Set(c.ctx.decs.Ascending, c.ctx.tr, c.onlyFirstOrLast, c.ops)
	c.ctx.decs.SetClosedSignal(c.ctx.closedSignal)
	filterOpts := immutable.NewFilterOpts(c.filter, &c.ctx.filterOption, c.tags, c.rowFilters)
	if orderLoc {
		return c.locations.ReadData(filterOpts, dst, nil, nil)
	}
	return c.outOfOrderLocations.ReadData(filterOpts, dst, nil, nil)
}

func (c *tsmMergeCursor) SetOps(ops []*comm.CallOption) {
	c.ops = append(c.ops, ops...)
	if len(ops) == 0 {
		return
	}
	name := ops[0].Call.Name
	if name == "first" {
		// is only first call
		c.onlyFirstOrLast = true
		for _, call := range ops {
			if call.Call.Name != "first" {
				c.onlyFirstOrLast = false
				break
			}
		}
	} else if name == "last" {
		// is only last call
		c.onlyFirstOrLast = true
		for _, call := range ops {
			if call.Call.Name != "last" {
				c.onlyFirstOrLast = false
				break
			}
		}
	}
}

func (c *tsmMergeCursor) Next() (*record.Record, error) {
	if c.ctx.IsAborted() {
		return nil, nil
	}

	if !c.init && c.lazyInit {
		c.locations = immutable.NewLocationCursor(len(c.ctx.readers.Orders))
		c.outOfOrderLocations = immutable.NewLocationCursor(len(c.ctx.readers.OutOfOrders))
		if e := c.AddLoc(); e != nil {
			return nil, e
		}
		c.init = true
	}
	var err error
	if c.recordPool == nil {
		c.recordPool = record.NewCircularRecordPool(c.ctx.tmsMergePool, tsmMergeCursorRecordNum, c.ctx.schema, false)
	}
	// First time read out of order data
	if !c.locationInit {
		if err = c.FirstTimeInit(); err != nil {
			return nil, err
		}
		c.locationInit = true
	}

	// Lazy out-of-order merge path: defer reading out-of-order data until the ordered watermark
	// advances. Falls back to the eager path below when not active.
	if c.lazyMerger != nil {
		return c.nextLazy()
	}

	if c.orderRecIter.hasRemainData() {
		rec := mergeData(&c.outOrderRecIter, &c.orderRecIter, c.ctx.maxRowCnt, c.ctx.decs.Ascending)
		return rec, nil
	}

	orderRec := c.recordPool.Get()
	newRec, err := c.readData(true, orderRec)
	if err != nil {
		return nil, err
	}

	c.orderRecIter.init(newRec)
	if len(c.ops) > 0 && c.outOrderRecIter.record != nil {
		if c.orderRecIter.record != nil {
			immutable.AggregateData(c.outOrderRecIter.record, c.orderRecIter.record, c.ops)
		}
		immutable.ResetAggregateData(c.outOrderRecIter.record, c.ops)
		c.orderRecIter.init(c.outOrderRecIter.record)
		c.outOrderRecIter.reset()
	}
	rec := mergeData(&c.outOrderRecIter, &c.orderRecIter, c.ctx.maxRowCnt, c.ctx.decs.Ascending)
	return rec, nil
}

// nextLazy drives the lazy out-of-order merge path: heap K-way merge of the out-of-order
// locations, streamed in maxRowCnt-sized batches, with no watermark.
//
// openGemini's out-of-order data is older than the ordered data (SplitRecordByTime at flush).
// The two are time-disjoint. For ascending, output = unordered(older) -> ordered(newer), so
// Phase 1 streams unordered then Phase 2 reads ordered. For descending the order is reversed:
// Phase 1 reads ordered (newer, output first), Phase 2 streams unordered (older).
//
// The underlying data scanning (segment reads, filtering) is direction-agnostic — Location's
// internal !Ascending branches handle segment iteration direction and FilterByTime(Descend).
// The heap's Less flips time comparison by the ascending flag. appendMergedSameTimeRow is
// direction-independent (same-time merge by seq precedence).
func (c *tsmMergeCursor) nextLazy() (*record.Record, error) {
	if c.ctx.IsAborted() {
		return nil, nil
	}

	if c.ctx.decs.Ascending {
		// Ascending: unordered (older) first, then ordered (newer).
		if c.lazyMerger != nil && !c.lazyMerger.allDone() {
			if rec, err := c.nextLazyUnorderedBatch(); err != nil || rec != nil {
				return rec, err
			}
		}
		return c.nextLazyOrdered()
	}

	// Descending: ordered (newer) first, then unordered (older).
	if rec, err := c.nextLazyOrdered(); err != nil || rec != nil {
		return rec, err
	}
	if c.lazyMerger != nil && !c.lazyMerger.allDone() {
		return c.nextLazyUnorderedBatch()
	}
	return nil, nil
}

// nextLazyUnorderedBatch streams one maxRowCnt-sized batch of out-of-order data via the heap
// merger into outOrderRecIter, then merges it via mergeData.
func (c *tsmMergeCursor) nextLazyUnorderedBatch() (*record.Record, error) {
	if c.outOrderRecIter.hasRemainData() {
		return mergeData(&c.outOrderRecIter, &c.orderRecIter, c.ctx.maxRowCnt, c.ctx.decs.Ascending), nil
	}
	c.ctx.decs.Set(c.ctx.decs.Ascending, c.ctx.tr, c.onlyFirstOrLast, c.ops)
	c.ctx.decs.SetClosedSignal(c.ctx.closedSignal)
	rec, err := c.lazyMerger.nextBatch(nil, c.ctx.maxRowCnt)
	if err != nil {
		return nil, err
	}
	if rec != nil {
		c.outOrderRecIter.init(rec)
		return mergeData(&c.outOrderRecIter, &c.orderRecIter, c.ctx.maxRowCnt, c.ctx.decs.Ascending), nil
	}
	return nil, nil
}

// nextLazyOrdered reads the next ordered batch and merges it via mergeData.
func (c *tsmMergeCursor) nextLazyOrdered() (*record.Record, error) {
	if !c.orderRecIter.hasRemainData() {
		orderRec := c.recordPool.Get()
		newRec, err := c.readData(true, orderRec)
		if err != nil {
			return nil, err
		}
		c.orderRecIter.init(newRec)
	}
	if c.orderRecIter.record == nil {
		return nil, nil
	}
	return mergeData(&c.outOrderRecIter, &c.orderRecIter, c.ctx.maxRowCnt, c.ctx.decs.Ascending), nil
}

func (c *tsmMergeCursor) reset() {
	c.ctx = nil
	c.span = nil
	c.onlyFirstOrLast = false
	c.ops = c.ops[:0]
	c.locations = nil
	c.outOfOrderLocations = nil
	c.locationInit = false

	c.filter = nil
	c.tags = nil
	c.sid = 0
	c.init = false
	c.lazyInit = false

	c.rowFilters = nil

	c.lazyMerger = nil

	c.orderRecIter.reset()
	// Reset the iters first so they drop references to any pool-held records (outRec may be a
	// unorderPool slot), then release the pool.
	c.outOrderRecIter.reset()

	if c.recordPool != nil {
		c.recordPool.Put()
		c.recordPool = nil
	}
	if c.unorderPool != nil {
		c.unorderPool.Put()
		c.unorderPool = nil
	}
}

func (c *tsmMergeCursor) Close() error {
	c.reset()
	putTsmCursor(c)
	return nil
}

func (c *tsmMergeCursor) StartSpan(span *tracing.Span) {
	c.span = span
}

func (c *tsmMergeCursor) EndSpan() {
}

func (c *tsmMergeCursor) FirstTimeOutOfOrderInit() error {
	if c.outOfOrderLocations.Len() == 0 {
		return nil
	}

	if c.outOfOrderLocations.Len() > 1 {
		sort.Sort(c.outOfOrderLocations)
	}
	var tm time.Time

	if c.span != nil {
		c.span.Count(tsmIterCount, 1)
		c.span.CreateCounter(unorderedLocationCount, "")
		c.span.Count(unorderedLocationCount, int64(c.outOfOrderLocations.Len()))
		tm = time.Now()
	}
	//isFirst := true
	var outRec *record.Record
	c.ctx.decs.Set(c.ctx.decs.Ascending, c.ctx.tr, c.onlyFirstOrLast, c.ops)
	filterOpts := immutable.NewFilterOpts(c.filter, &c.ctx.filterOption, c.tags, c.rowFilters)
	dst := record.NewRecordBuilder(c.ctx.schema)
	rec, err := c.outOfOrderLocations.ReadOutOfOrderMeta(filterOpts, dst)
	if err != nil {
		return err
	}
	outRec = rec

	if c.span != nil {
		c.span.Count(unorderRowCount, int64(outRec.RowNums()))
		c.span.Count(unorderDuration, int64(time.Since(tm)))
	}

	c.outOrderRecIter.init(outRec)
	return nil
}

func (c *tsmMergeCursor) FirstTimeInit() error {
	if c.locations.Len() > 1 {
		sort.Sort(c.locations)
		if !c.ctx.decs.Ascending {
			c.locations.Reverse()
		}
	}

	if len(c.ops) > 0 {
		e := c.FirstTimeOutOfOrderInit()
		if e != nil {
			return e
		}
		return nil
	}

	if c.outOfOrderLocations.Len() == 0 {
		return nil
	}

	// Lazy path: prepare the merger but do not read out-of-order data up front. Data is read in
	// watermark-bounded batches from Next(). Falls back to the eager path below for any query
	// shape not supported by the lazy merger.
	if c.lazyUnorderedEnabled() {
		if c.outOfOrderLocations.Len() > 1 {
			sort.Sort(c.outOfOrderLocations)
		}
		c.ctx.decs.Set(c.ctx.decs.Ascending, c.ctx.tr, c.onlyFirstOrLast, c.ops)
		c.lazyMerger = newLazyUnorderedMerger(c.ctx.schema, c.newFilterOpts(), c.outOfOrderLocations, c.ctx.IsAborted, c.ctx.decs.Ascending)
		if c.span != nil {
			c.span.Count(tsmIterCount, 1)
			c.span.CreateCounter(unorderedLocationCount, "")
			c.span.Count(unorderedLocationCount, int64(c.outOfOrderLocations.Len()))
		}
		return nil
	}

	if c.outOfOrderLocations.Len() > 1 {
		sort.Sort(c.outOfOrderLocations)
	}
	var tm time.Time

	if c.span != nil {
		c.span.Count(tsmIterCount, 1)
		c.span.CreateCounter(unorderedLocationCount, "")
		c.span.Count(unorderedLocationCount, int64(c.outOfOrderLocations.Len()))
		c.span.CreateCounter(unorderedMergeCount, "")
		tm = time.Now()
	}
	isFirst := true
	var outRec *record.Record
	if c.unorderPool == nil {
		c.unorderPool = record.NewCircularRecordPool(c.ctx.tmsMergePool, unorderRecordNum, c.ctx.schema, false)
	}
	for {
		// dst is reused from the pool instead of allocating a NewRecordBuilder every iteration.
		// dst is only ever used as the read buffer and then as the newRec/oldRec argument to
		// MergeRecord (read-only); it is never used as the MergeRecord receiver, whose schema
		// mergeRecordSchema appends to (so a pre-populated pool record must not be a receiver).
		dst := c.unorderPool.Get()
		rec, err := c.readData(false, dst)
		if err != nil {
			return err
		}
		// end of cursor
		if rec == nil {
			break
		}
		if isFirst {
			outRec = rec
		} else {
			if c.span != nil {
				c.span.Count(unorderedMergeCount, 1)
			}
			var mergeRecord record.Record
			if c.ctx.decs.Ascending {
				mergeRecord.MergeRecord(rec, outRec)
			} else {
				mergeRecord.MergeRecordDescend(rec, outRec)
			}

			outRec = &mergeRecord
		}
		isFirst = false
	}

	if c.span != nil {
		c.span.Count(unorderRowCount, int64(outRec.RowNums()))
		c.span.Count(unorderDuration, int64(time.Since(tm)))
	}

	c.outOrderRecIter.init(outRec)

	return nil
}
