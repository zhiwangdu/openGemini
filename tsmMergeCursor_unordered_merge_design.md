# tsmMergeCursor 乱序合并优化设计

本文档描述 `tsmMergeCursor.FirstTimeInit` 乱序查询路径的优化实现。它是
`tsmMergeCursor_FirstTimeInit_unordered_analysis.md`（分析文档）落地后的最终设计，
已根据 review 和 benchmark 结果对目标与方案做了修正。

## 1. 目标

优化针对两个明确问题（**不是**首包延迟——查询通常迭代完所有数据才返回，首包不是观测指标）：

1. **内存/GC 压力**：原 `FirstTimeInit` 把所有命中的乱序 location 读空，链式合并成一个
   完整 `outRec`。乱序数据多时，`outRec` 巨大，内存峰值高、GC 抖动。
2. **总查询性能差**：原合并是“读一个 record，与累计 outRec 合并一次”的单路链式归并，
   复杂度 `O(N²·R)`（N 个乱序文件、每文件 R 行）。乱序文件多时查询明显变慢。

**成功指标**：在乱序文件较多的场景下，总查询耗时下降、峰值内存下降；在乱序文件少的
场景下不退化。

## 2. 问题根因（源码位置）

- `engine/tsm_merge_cursor.go` `FirstTimeInit`：非聚合路径循环 `readData(false, dst)` 直到
  EOF，每次用 `MergeRecord(rec, outRec)` 链式合并，`outRec = &mergeRecord`。
- `outRec` 被 `outOrderRecIter` 持有，直到 `mergeData` 消耗完。
- `ReInit`/`ReInitWithShard` 不重置 `locationInit`/`lazyMerger`，复用 cursor 时乱序状态残留。

> 注：分析文档曾称 `outRec == nil` 时 `outRec.RowNums()` 会 panic。实际 `record.RowNums` 已对
> `rec == nil` 判空返回 0，**不会 panic**。该“nil guard”修复为误判，已回退（见 §4.1）。

## 3. 方案总览

分两层，均落在 `engine/` 和 `engine/immutable/`：

| 层 | 范围 | 默认 | 状态 |
|---|---|---|---|
| Phase 1 | 观测指标 + dst 复用 | 始终启用 | 已上线 |
| Phase 3 | 惰性堆式 K 路乱序合并 | feature flag，默认关 | 已实现，灰度 |

Phase 3 是核心：用 **watermark-bounded 的堆 K 路合并** 替换“全量预读 + 链式合并”。
未实现 Phase 2（schema/field 预过滤）和 Phase 4（后台分层合并），见 §10。

## 4. Phase 1：安全改进（默认启用）

### 4.1 nil-outRec 路径（无需修复）
当所有乱序 location 被过滤为 nil 时 `outRec` 为 nil。`record.RowNums` 对 `rec == nil` 判空
返回 0，`recordIter.init(nil)` 也安全，因此 span 计数与 `init` 均不会 panic。分析文档关于
panic 的判断有误，对应的“nil guard”提交已回退。`TestFirstTimeInitEmptyOutOfOrder` 仍覆盖该
路径（验证 `Next` 返回 nil 且计数正确）。

### 4.2 观测指标
新增 span 计数（`engine/iterators.go` 常量，`FirstTimeInit` 内 `CreateCounter` 幂等创建）：
- `unordered_location_count`：命中的乱序 location 数（放大因子 K）。
- `unordered_merge_count`：非聚合路径的链式合并次数。

### 4.3 dst 复用
非聚合 `FirstTimeInit` 循环里的 `dst := record.NewRecordBuilder(...)` 改为从 `unorderPool`
（环形，`unorderRecordNum = 2`）获取。**关键约束**：`record.Record.mergeRecordSchema` 向
receiver 的 schema **追加**，因此池化的 record 只能作为 `MergeRecord` 的 `newRec`/`oldRec`
参数（只读），**不能**作为 receiver。合并 scratch 仍用 `var mergeRecord record.Record`。
`reset()` 先 `outOrderRecIter.reset()`（释放对池槽的引用）再 `unorderPool.Put()`。

## 5. Phase 3：惰性堆式 K 路合并

### 5.1 核心思路
不再在 `FirstTimeInit` 一次性读完所有乱序数据。改为：每个有序 batch 决定一个
watermark（升序 = 该 batch 的最大时间），只把时间 `<= watermark` 的乱序数据读出并合并，
交给已有的 `mergeData`（`MergeRecordByMaxTimeOfOldRec`）与有序数据合并。乱序数据按
watermark 分批读入，**峰值内存受 batch + 堆中 live 源数限制**，而非全部乱序数据量。

合并算法用 **`container/heap` K 路归并**，复杂度 `O(M·log K)`（M = N·R 总行数），替换链式
`O(N²·R)`。

### 5.2 组件

- `engine/unordered_lazy_merge.go` `lazyUnorderedMerger`：
  - 持有 `sources []*unorderedSource`（每个乱序 location 一个）和 `heap lazyMergerHeap`。
  - `unorderedSource`：`loc`、`seq`（文件序列号，越大越新）、`rec`、`pos`、`done`、`inHeap`。
  - `nextBatchUntil(watermark)`：emit **所有** `time <= watermark` 的行（无行数上限，正确性
    要求：`mergeData` 必须看到该 watermark 内全部同时间点乱序行）。
  - `nextBatch(nil, maxRows)`：有序耗尽后的乱序-only 路径，按 `maxRows` 分批。
  - 堆序：按当前行时间升序，同时间按 `seq` 降序（最新者先 pop）。
  - `appendMergedSameTimeRow`：同时间组按 newest→oldest 折叠，每列取最新非 nil 值，等价于
    `mergeRecRow` 折叠。所有输入 record 共享 `ctx.schema`，按列下标对齐。
  - `isAborted` 回调：合并循环顶检查，abort 时 `return nil, nil`（丢弃半成品）。

- `engine/immutable/location.go` `ReadDataBeforeWatermark(filterOpts, dst, watermark)`：
  读下一个“时间 `<= watermark` 且与查询时间范围重叠”的 segment，应用与 `ReadData` 相同的
  `FilterByTime`/`FilterByField`；不重叠查询范围的 segment 跳过（推进），首个超过 watermark
  的 segment **保留不动**并返回 nil（延迟到 watermark 推进后再读）。仅支持升序。
  另暴露 `Location.HasNext()`、`Location.Sequence()`、`LocationCursor.LocationAt(i)`。

- `engine/tsm_merge_cursor.go`：
  - `FirstTimeInit` 惰性分支：排序乱序 locations、`Set` decs、创建 `lazyMerger`，**不读数据**。
  - `nextLazy()`：若有序 batch 有剩余且 ready buffer 空，先 `ensureUnorderedReadyUntil(watermark)`
    再 `mergeData`；有序耗尽则走 `nextUnorderedOnlyLazy()`。
  - `ensureUnorderedReadyUntil(watermark)`：ready buffer 非空时直接返回（所有权不变式）；否则
    `lazyMerger.nextBatchUntil(watermark)` 产出全部 `<= watermark` 的乱序行 init 进 `outOrderRecIter`。
  - `nextUnorderedOnlyLazy()`：乱序-only drain，`nextBatch(nil, maxRowCnt)` 分批。

### 5.3 正确性不变式

1. **Watermark**：返回任何 `t <= watermark` 的行前，所有可能产生该时间的乱序 location 已被
   读取并进入 merger。`nextBatchUntil` 无行数上限地 emit 全部 `<= watermark` 的行，保证
   `mergeData` 不漏同时间点乱序覆盖。
2. **同时间覆盖**：堆按 `seq` 降序 pop 同时间组，`appendMergedSameTimeRow` newest 非 nil 胜出，
   与 eager 链式 `MergeRecord(newRec=高seq, oldRec=累计)` 语义等价。
3. **Ready buffer 所有权**：`ensureUnorderedReadyUntil` 在 `outOrderRecIter.hasRemainData()` 时
   直接返回，绝不覆盖未消费完的 batch（`mergeData` 可能因 `maxRowCnt` 只消费一部分）。
4. **有序耗尽 fallback**：`nextUnorderedOnlyLazy` 在有序读完后 drain 剩余乱序，按 `maxRows` 分批。
5. **终止**：堆空且所有源 `done`/`deferred` 时 `nextBatch` 返回 nil；`allDone()` 判定 drain 完成，
   无死循环。

### 5.4 小 N 阈值（防退化）
`lazyUnorderedMergeMinLocations`（默认 64，`atomic.Int32`，`SetLazyUnorderedMergeMinLocations` 可调）：
命中的乱序 location 数低于阈值时回退 eager。benchmark 显示交叉点约 N=100（N=10 时惰性慢 2.2×，
N=100 持平，N=1000 惰性快 5×）。阈值保证开启 flag **不退化**小 N 常见场景。0 表示禁用阈值
（测试用）。

### 5.5 Feature flag
`lazyUnorderedMergeEnabled`（`atomic.Bool`，默认关）：`SetLazyUnorderedMergeEnabled` 运行时切换。
`lazyUnorderedEnabled()` 还限制：仅升序、非聚合（`len(ops)==0`）、非 limit-cut、非 Prom。

## 6. 生命周期：ReInit 修复

`ReInit`/`ReInitWithShard` 复用 cursor 给新 series 时，新增 `c.locationInit = false; c.lazyMerger = nil`，
使新 series 重新跑 `FirstTimeInit` 处理自己的乱序数据。修复两个问题：
- **惰性路径（回归）**：否则 `nextLazy` 会把上一个 series 残留的 stale `lazyMerger` 源（指向
  已孤立的 Location）排进新 series 输出 → 跨 series 数据错乱。
- **eager 路径（预存）**：`locationInit` 残留导致 `FirstTimeInit` 被跳过，新 series 乱序数据不读。

## 7. 正确性验证：差分测试

`engine/tsm_merge_cursor_test.go`：以 eager 路径为 oracle，对 lazy 路径逐行比较
`(time, value, isNil)`。
- `TestLazyUnorderedMergeDifferential`：7 个固定 edge case + 3000 个随机用例（1-3 有序文件、
  1-4 乱序文件、文件内时间严格递增、含 nil 值、`maxRowCnt` 1-4 强制分批）。
- `TestLazyUnorderedMergeMultiSegment`：多 segment 文件（`mocTsspFileMultiSeg` +
  `immutable.NewChunkMetaWithSegs`），覆盖 `ReadDataBeforeWatermark` 的 segment 跳过/延迟。
- 覆盖：仅有序、仅乱序（有序耗尽 fallback）、同时间高 seq 覆盖、nil 列由旧源填补、无重叠、
  小批 ready-buffer 所有权、乱序跨多个有序 batch。

差分测试覆盖**新增逻辑**（merger、watermark、admission）；文件 I/O/过滤复用 eager 路径未改。
`-race` 通过。

## 8. 性能（benchmark）

`engine/tsm_merge_cursor_bench_test.go`，mock 读（无 I/O，反映 CPU/分配；真实 I/O 节省另算）。
阈值置 0 以测量纯 lazy 路径。

`BenchmarkTotal_NoOverlap`（drain 到完成 = 真实查询指标）：

| N | Eager | Lazy | 时间 | 峰值内存 |
|---|---|---|---|---|
| 10 | 19.8 µs | 43.6 µs | lazy 慢（小 N → 生产由阈值路由到 eager） | 42 KB vs 28 KB |
| 100 | 589 µs | 618 µs | **持平** | 2.1 MB → 302 KB（**7×**） |
| 1000 | 43.7 ms | 8.45 ms | **5.2× 更快** | 175 MB → 3 MB（**58×**） |

- 目标 1（内存/GC）：N≥100 时峰值内存降 7–58×。
- 目标 2（总性能）：N=1000 快 5.2×；小 N 由阈值保护不退化。
- 惰性路径首包/内存随 N 近线性，eager 随 N 超线性（全量预读 + O(N²) 链式合并）。

## 9. 已知限制

- **仅升序**：降序/聚合/limit-cut/Prom 回退 eager。
- **全重叠场景**（所有乱序与有序同时间）：惰性无延迟收益，且 per-row `AppendColVal` 合并慢于
  eager 的向量化 `MergeRecord`，同时堆中需持有全部 K 个源 → 时间与内存均无优势。这是非典型的
  人造最坏情况；真实乱序写入时间分散（部分/无重叠），惰性为净收益。待办：重叠感知 fallback。
- **flag 未接 config**：目前仅运行时 setter（同 `EnableFileCursor` 模式），未接 `Query` 配置项。

## 10. 未实现 / 待办

- **Phase 2**（`AddLocations` 的 schema/field 预过滤）：分析文档指出 count(time)/aux/Prom 语义需
  进一步验证；惰性路径已按 watermark 跳过未来 segment，边际收益变小。
- **Phase 4**（乱序文件过多的后台分层合并/预合并）：长期 compaction 侧工作，超出查询路径范围。
- **全重叠 fallback**：见 §9。
- **config 接入**：见 §9。
- **merger 池化**：源 record 与输出 batch 仍用 `NewRecordBuilder`，可进一步池化降分配。

## 11. 文件清单

| 文件 | 改动 |
|---|---|
| `engine/tsm_merge_cursor.go` | 计数、`unorderPool`、`lazyMerger`/`nextLazy`/`ensureUnorderedReadyUntil`/`nextUnorderedOnlyLazy`、flag、阈值、ReInit 重置 |
| `engine/unordered_lazy_merge.go` | 新增：堆 K 路 merger |
| `engine/immutable/location.go` | `ReadDataBeforeWatermark`、`HasNext`、`Sequence` |
| `engine/immutable/location_cursor.go` | `LocationAt` |
| `engine/immutable/tssp_file_meta.go` | `NewChunkMetaWithSegs`（测试用多段构造） |
| `engine/iterators.go` | 计数常量、`unorderRecordNum` |
| `engine/tsm_merge_cursor_test.go` | 差分测试、多段测试、mock |
| `engine/tsm_merge_cursor_bench_test.go` | benchmark |

## 12. 提交历史（branch `merge-optimize`）

1. `fix(engine): guard nil outRec in tsmMergeCursor FirstTimeInit span counting`（误判，已由 12 回退）
2. `feat(engine): add unordered location/merge counters to tsmMergeCursor`
3. `perf(engine): reuse dst builder in tsmMergeCursor FirstTimeInit unordered loop`
4. `feat(immutable): add Location watermark-bounded read + accessors for lazy unordered merge`
5. `feat(engine): lazy out-of-order merge path in tsmMergeCursor (flag-gated, default off)`
6. `test(engine): differential correctness tests for lazy out-of-order merge`
7. `fix(engine): reset out-of-order state on tsmMergeCursor ReInit/ReInitWithShard`
8. `refactor(engine): harden lazy unordered merge per review (atomic flag, abort, multiseg tests)`
9. `fix(engine): discard partial lazy merge batch on query abort`
10. `test(engine): benchmarks for lazy vs eager unordered merge`
11. `perf(engine): heap K-way merge + small-N threshold for lazy unordered merge`
12. `revert(engine): remove no-op nil-outRec guard (RowNums is nil-safe)`
