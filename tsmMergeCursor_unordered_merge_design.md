# tsmMergeCursor 乱序合并优化设计

本文档描述 `tsmMergeCursor.FirstTimeInit` 乱序查询路径的优化实现与后续规划。它是
`tsmMergeCursor_FirstTimeInit_unordered_analysis.md`（分析文档）落地后的最终设计，已根据
review、benchmark 和补充分析对目标与方案做了修正。

---

## 1. 背景与目标

优化针对两个明确问题（**不是**首包延迟——查询通常迭代完所有数据才返回，首包不是观测指标）：

1. **内存/GC 压力**：原 `FirstTimeInit` 把所有命中的乱序 location 读空，链式合并成一个完整
   `outRec`。乱序数据多时 `outRec` 巨大，内存峰值高、GC 抖动。
2. **总查询性能差**：原合并是“读一个 record，与累计 outRec 合并一次”的单路链式归并，复杂度
   `O(N²·R)`（N 个乱序文件、每文件 R 行）。乱序文件多时查询明显变慢。

**成功指标**：乱序文件较多的场景下，总查询耗时下降、峰值内存下降；乱序文件少时不退化。

## 2. 问题根因（源码位置）

- `engine/tsm_merge_cursor.go` `FirstTimeInit`：非聚合路径循环 `readData(false, dst)` 直到 EOF，
  每次用 `MergeRecord(rec, outRec)` 链式合并，`outRec = &mergeRecord`。
- `outRec` 被 `outOrderRecIter` 持有，直到 `mergeData` 消耗完。
- `ReInit`/`ReInitWithShard` 不重置 `locationInit`/`lazyMerger`，复用 cursor 时乱序状态残留。

> 注：分析文档曾称 `outRec == nil` 时 `outRec.RowNums()` 会 panic。实际 `record.RowNums` 已对
> `rec == nil` 判空返回 0，**不会 panic**。该“nil guard”修复为误判，已回退（见 §4.1）。

## 3. 方案总览

分两层，均落在 `engine/` 和 `engine/immutable/`：

| 层 | 范围 | 默认 | 状态 |
|---|---|---|---|
| Phase 1 | 观测指标 + dst 复用 | 始终启用 | 已上线 |
| Phase 3 | 惰性堆式 K 路乱序合并 | feature flag，默认关 | 已实现（仅升序非聚合），灰度 |

Phase 3 是核心：用 **watermark-bounded 的堆 K 路合并** 替换“全量预读 + 链式合并”。未实现
Phase 2（schema/field 预过滤）、Phase 4（后台分层合并）、降序与 limit-cut 的 lazy 支持，见 §10。

## 4. Phase 1：安全改进（默认启用）

### 4.1 nil-outRec 路径（无需修复）
当所有乱序 location 被过滤为 nil 时 `outRec` 为 nil。`record.RowNums` 对 `rec == nil` 判空返回 0，
`recordIter.init(nil)` 也安全，因此 span 计数与 `init` 均不会 panic。分析文档关于 panic 的判断有误，
对应的“nil guard”提交已回退。`TestFirstTimeInitEmptyOutOfOrder` 仍覆盖该路径（验证 `Next` 返回 nil
且计数正确）。

### 4.2 观测指标
新增 span 计数（`engine/iterators.go` 常量，`FirstTimeInit` 内 `CreateCounter` 幂等创建）：
- `unordered_location_count`：命中的乱序 location 数（放大因子 K）。
- `unordered_merge_count`：非聚合路径的链式合并次数。

### 4.3 dst 复用
非聚合 `FirstTimeInit` 循环里的 `dst := record.NewRecordBuilder(...)` 改为从 `unorderPool`（环形，
`unorderRecordNum = 2`）获取。**关键约束**：`record.Record.mergeRecordSchema` 向 receiver 的 schema
**追加**，因此池化的 record 只能作为 `MergeRecord` 的 `newRec`/`oldRec` 参数（只读），**不能**作为
receiver。合并 scratch 仍用 `var mergeRecord record.Record`。`reset()` 先 `outOrderRecIter.reset()`
（释放对池槽的引用）再 `unorderPool.Put()`。

## 5. Phase 3：惰性堆式 K 路合并（flag 灰度）

### 5.1 核心思路
不在 `FirstTimeInit` 一次性读完所有乱序数据。每个有序 batch 决定一个 watermark（升序 = 该 batch
的最大时间），只把时间 `<= watermark` 的乱序数据读出并合并，交给已有的 `mergeData`
（`MergeRecordByMaxTimeOfOldRec`）与有序数据合并。乱序数据按 watermark 分批读入，**峰值内存受
batch + 堆中 live 源数限制**，而非全部乱序数据量。合并用 **`container/heap` K 路归并**，复杂度
`O(M·log K)`（M = N·R 总行数），替换链式 `O(N²·R)`。

### 5.2 组件

- `engine/unordered_lazy_merge.go` `lazyUnorderedMerger`：
  - 持有 `sources []*unorderedSource`（每个乱序 location 一个）和 `heap lazyMergerHeap`。
  - `unorderedSource`：`loc`、`seq`（文件序列号，越大越新）、`rec`、`pos`、`done`、`inHeap`。
  - `nextBatchUntil(watermark)`：emit **所有** `time <= watermark` 的行（无行数上限，正确性要求：
    `mergeData` 必须看到该 watermark 内全部同时间点乱序行）。
  - `nextBatch(nil, maxRows)`：有序耗尽后的乱序-only 路径，按 `maxRows` 分批。
  - 堆序：当前行时间升序，同时间 `seq` 降序（最新者先 pop）。
  - `appendMergedSameTimeRow`：同时间组按 newest→oldest 折叠，每列取最新非 nil 值，等价于
    `mergeRecRow` 折叠。所有输入 record 共享 `ctx.schema`，按列下标对齐。
  - `isAborted` 回调：合并循环顶检查，abort 时 `return nil, nil`（丢弃半成品）。

- `engine/immutable/location.go` `ReadDataBeforeWatermark(filterOpts, dst, watermark)`：读下一个
  “时间 `<= watermark` 且与查询时间范围重叠”的 segment，应用与 `ReadData` 相同的
  `FilterByTime`/`FilterByField`；不重叠查询范围的 segment 跳过（推进），首个超过 watermark 的
  segment **保留不动**并返回 nil（延迟到 watermark 推进后再读）。**仅支持升序**。另暴露
  `Location.HasNext()`、`Location.Sequence()`、`LocationCursor.LocationAt(i)`。

- `engine/tsm_merge_cursor.go`：
  - `FirstTimeInit` 惰性分支：排序乱序 locations、`Set` decs、创建 `lazyMerger`，**不读数据**。
  - `nextLazy()`：若有序 batch 有剩余且 ready buffer 空，先 `ensureUnorderedReadyUntil(watermark)`
    再 `mergeData`；有序耗尽则走 `nextUnorderedOnlyLazy()`。
  - `ensureUnorderedReadyUntil(watermark)`：ready buffer 非空时直接返回（所有权不变式）；否则
    `lazyMerger.nextBatchUntil(watermark)` 产出全部 `<= watermark` 的乱序行 init 进 `outOrderRecIter`。
  - `nextUnorderedOnlyLazy()`：乱序-only drain，`nextBatch(nil, maxRowCnt)` 分批。

### 5.3 正确性不变式

1. **Watermark**：返回任何 `t <= watermark` 的行前，所有可能产生该时间的乱序 location 已被读取并
   进入 merger。`nextBatchUntil` 无行数上限地 emit 全部 `<= watermark` 的行，保证 `mergeData` 不漏
   同时间点乱序覆盖。
2. **同时间覆盖**：堆按 `seq` 降序 pop 同时间组，`appendMergedSameTimeRow` newest 非 nil 胜出，与
   eager 链式 `MergeRecord(newRec=高seq, oldRec=累计)` 语义等价。
3. **Ready buffer 所有权**：`ensureUnorderedReadyUntil` 在 `outOrderRecIter.hasRemainData()` 时直接
   返回，绝不覆盖未消费完的 batch（`mergeData` 可能因 `maxRowCnt` 只消费一部分）。
4. **有序耗尽 fallback**：`nextUnorderedOnlyLazy` 在有序读完后 drain 剩余乱序，按 `maxRows` 分批。
5. **终止**：堆空且所有源 `done`/`deferred` 时 `nextBatch` 返回 nil；`allDone()` 判定 drain 完成，
   无死循环。

### 5.4 小 N 阈值（防退化）
`lazyUnorderedMergeMinLocations`（默认 64，`atomic.Int32`，`SetLazyUnorderedMergeMinLocations` 可调）：
命中的乱序 location 数低于阈值时回退 eager。benchmark 显示交叉点约 N=100（N=10 惰性慢 2.2×，
N=100 持平，N=1000 惰性快 5×）。阈值保证开启 flag **不退化**小 N 常见场景。0 表示禁用阈值（测试用）。

### 5.5 Feature flag 与适用范围
`lazyUnorderedMergeEnabled`（`atomic.Bool`，默认关）：`SetLazyUnorderedMergeEnabled` 运行时切换。
`lazyUnorderedEnabled()` 限制：**仅升序**、非聚合（`len(ops)==0`）、非 limit-cut、非 Prom。其余形状
回退 eager。降序与 limit-cut 的放开见 §10.2/§10.3。

### 5.6 流程图（优化后）

#### 5.6.1 惰性路径的初始化与迭代

```mermaid
flowchart TD
    A["newTsmMergeCursor(ctx, sid)"] --> B["AddLoc: ordered + unordered LocationCursor"]
    B --> O["First Next"]
    O --> P["FirstTimeInit"]
    P --> Q{"lazyUnorderedEnabled?"}
    Q -- "no (eager, 默认)" --> S["sort + read ALL unordered until EOF + chain merge → outRec"]
    Q -- "yes" --> R["sort unordered locations; newLazyUnorderedMerger (不读数据)"]
    S --> T["locationInit = true"]
    R --> T

    T --> N["Next"]
    N --> G{"lazyMerger != nil?"}
    G -- "no" --> E0["eager: mergeData(outOrderRecIter, orderRecIter)"]
    G -- "yes" --> L["nextLazy"]
    L --> L1{"orderRecIter has remain?"}
    L1 -- "yes, outOrder empty" --> L2["ensureUnorderedReadyUntil(ordered watermark)"]
    L1 -- "yes, outOrder has remain" --> L4["mergeData"]
    L1 -- "no" --> L3["read next ordered batch"]
    L3 --> L5{"ordered record nil?"}
    L5 -- "yes (exhausted)" --> L6["nextUnorderedOnlyLazy: drain remaining unordered"]
    L5 -- "no" --> L2
    L2 --> L4
    L4 --> OUT["record to seriesCursor"]

    L2 --> M1["lazyMerger.nextBatchUntil(watermark)"]
    M1 --> M2["ReadDataBeforeWatermark per location (only segments minT <= watermark)"]
    M2 --> M3["heap K-way merge: same-time group + seq precedence"]
    M3 --> M4["outOrderRecIter.init(ready batch)"]

    S:::hot
    R:::amp
    M2:::io
    M3:::hot
    L6:::amp
    classDef hot fill:#ffd6d6,stroke:#c62828,stroke-width:2px,color:#111;
    classDef amp fill:#fff0c2,stroke:#b26a00,stroke-width:2px,color:#111;
    classDef io fill:#d9e8ff,stroke:#1565c0,stroke-width:2px,color:#111;
```

标注：
- eager 分支（默认）保留原 `FirstTimeInit` 全量读 + 链式合并。
- 惰性分支在 `FirstTimeInit` 只建 `lazyMerger`，**不读乱序数据**；乱序按 ordered watermark 分批读。
- 有序耗尽走 `nextUnorderedOnlyLazy` drain 剩余乱序。

#### 5.6.2 惰性堆式 K 路合并数据流

```mermaid
flowchart LR
    U1["unordered file 1"] --> L1["Location 1"]
    U2["unordered file 2"] --> L2["Location 2"]
    UN["unordered file N"] --> LN["Location N"]

    L1 --> W["ReadDataBeforeWatermark (segment minT <= watermark)"]
    L2 --> W
    LN --> W
    W --> H["heap K-way: pop min time, same-time group by seq desc"]
    H --> SG["appendMergedSameTimeRow: newest non-nil wins"]
    SG --> RB["outOrderRecIter (ready batch, <= watermark)"]

    OF["ordered file stream"] --> OC["ordered LocationCursor.ReadData"]
    OC --> OR["orderRecIter"]
    OR --> WM["watermark = ordered batch max time"]
    WM --> W
    RB --> MD["mergeData(outOrderRecIter, orderRecIter)"]
    OR --> MD
    MD --> OUT["record to seriesCursor"]

    W:::io
    H:::hot
    SG:::cpu
    RB:::mem
    MD:::cpu
    WM:::amp
    classDef hot fill:#ffd6d6,stroke:#c62828,stroke-width:2px,color:#111;
    classDef io fill:#d9e8ff,stroke:#1565c0,stroke-width:2px,color:#111;
    classDef cpu fill:#e5ffd8,stroke:#2e7d32,stroke-width:2px,color:#111;
    classDef mem fill:#f3e5ff,stroke:#6a1b9a,stroke-width:2px,color:#111;
    classDef amp fill:#fff0c2,stroke:#b26a00,stroke-width:2px,color:#111;
```

标注（对比 §4.3 旧路径）：
- 旧：`LocationCursor.ReadData` 读到 EOF，链式 `MergeRecord(rec, outRec)` 累计拷贝 `O(N²R)`，`outRec` = 全部乱序。
- 新：`ReadDataBeforeWatermark` 只读 `minT <= watermark` 的 segment；堆 K 路合并 `O(M·logK)`；ready batch 仅含 `<= watermark` 的行。
- watermark 由 ordered batch 驱动；超过 watermark 的 segment 保留不动，延迟到下个 batch。

#### 5.6.3 watermark 推进与乱序延迟（eager vs lazy 对比）

```mermaid
flowchart TD
    subgraph OLD["eager（旧）"]
        OA["ordered batch 1"] --> OB["FirstTimeInit: 读全部 N 个乱序文件"]
        OB --> OC["chain merge O(N²R)"]
        OC --> OD["outRec = 全部乱序"]
        OD --> OE["mergeData batch 1"]
    end
    subgraph NEW["lazy（优化后）"]
        NA["ordered batch 1 (watermark = maxT1)"] --> NB["admit unordered segments <= maxT1"]
        NB --> NC["heap K-way merge (仅 admitted)"]
        NC --> ND["mergeData batch 1"]
        ND --> NE["ordered batch 2 (watermark = maxT2 > maxT1)"]
        NE --> NF["admit unordered in (maxT1, maxT2]"]
        NF --> NG["heap merge"]
        NG --> NH["mergeData batch 2"]
        NH --> NI["ordered exhausted"]
        NI --> NJ["drain remaining unordered"]
    end
    OB:::hot
    OC:::hot
    OD:::mem
    NB:::io
    NC:::hot
    NF:::io
    NJ:::amp
    classDef hot fill:#ffd6d6,stroke:#c62828,stroke-width:2px,color:#111;
    classDef io fill:#d9e8ff,stroke:#1565c0,stroke-width:2px,color:#111;
    classDef mem fill:#f3e5ff,stroke:#6a1b9a,stroke-width:2px,color:#111;
    classDef amp fill:#fff0c2,stroke:#b26a00,stroke-width:2px,color:#111;
```

标注：
- eager 在首批前读完全部乱序、构造完整 `outRec`（内存峰值 = 全部乱序 `O(F·U)`）。
- lazy 按 ordered watermark 分批准入，峰值 = 当前 watermark 内乱序 + batch；乱序多在后续时段时首批收益最大（见 §8 benchmark：N=1000 时 5.2× 更快、58× 省内存）。

## 6. 生命周期：ReInit 修复

`ReInit`/`ReInitWithShard` 复用 cursor 给新 series 时，新增 `c.locationInit = false; c.lazyMerger = nil`，
使新 series 重新跑 `FirstTimeInit` 处理自己的乱序数据。修复两个问题：
- **惰性路径（回归）**：否则 `nextLazy` 会把上一个 series 残留的 stale `lazyMerger` 源（指向已孤立的
  Location）排进新 series 输出 → 跨 series 数据错乱。
- **eager 路径（预存）**：`locationInit` 残留导致 `FirstTimeInit` 被跳过，新 series 乱序数据不读。

## 7. 正确性验证：差分测试

`engine/tsm_merge_cursor_test.go`：以 eager 路径为 oracle，对 lazy 路径逐行比较
`(time, value, isNil)`。
- `TestLazyUnorderedMergeDifferential`：7 个固定 edge case + 3000 个随机用例（1-3 有序文件、1-4 乱序
  文件、文件内时间严格递增、含 nil 值、`maxRowCnt` 1-4 强制分批）。
- `TestLazyUnorderedMergeMultiSegment`：多 segment 文件（`mocTsspFileMultiSeg` +
  `immutable.NewChunkMetaWithSegs`），覆盖 `ReadDataBeforeWatermark` 的 segment 跳过/延迟。
- 覆盖：仅有序、仅乱序（有序耗尽 fallback）、同时间高 seq 覆盖、nil 列由旧源填补、无重叠、小批
  ready-buffer 所有权、乱序跨多个有序 batch。

差分测试覆盖**新增逻辑**（merger、watermark、admission）；文件 I/O/过滤复用 eager 路径未改。
`-race` 通过。**注意**：测试用 mock 的 segment 时间是手工构造的有序序列，未覆盖真实文件的 segment
排序保证，见 §9。

## 8. 性能（benchmark）

`engine/tsm_merge_cursor_bench_test.go`，mock 读（无 I/O，反映 CPU/分配；真实 I/O 节省另算）。阈值
置 0 以测量纯 lazy 路径。

`BenchmarkTotal_NoOverlap`（drain 到完成 = 真实查询指标）：

| N | Eager | Lazy | 时间 | 峰值内存 |
|---|---|---|---|---|
| 10 | 19.8 µs | 43.6 µs | lazy 慢（小 N → 生产由阈值路由到 eager） | 42 KB vs 28 KB |
| 100 | 589 µs | 618 µs | **持平** | 2.1 MB → 302 KB（**7×**） |
| 1000 | 43.7 ms | 8.45 ms | **5.2× 更快** | 175 MB → 3 MB（**58×**） |

- 目标 1（内存/GC）：N≥100 时峰值内存降 7–58×。
- 目标 2（总性能）：N=1000 快 5.2×；小 N 由阈值保护不退化。
- 惰性路径首包/内存随 N 近线性，eager 随 N 超线性（全量预读 + O(N²) 链式合并）。

### 8.1 复杂度对比图

链式 `O(N²·R)` 与堆式 `O(M·logK)=O(N·R·log₂N)` 在同一对数纵轴上随 N 的增长对比
（R=20，理论常数=1；交互版见 `unordered_merge_complexity_chart.html`）：

![乱序合并复杂度对比](unordered_merge_complexity_chart.png)

纯复杂度比 `N/log₂N`：N=100 约 15×、N=1000 约 100×；实测（`Total_NoOverlap`）因常数因子
（链式 `MergeRecord` 向量化双指针 vs 堆 per-row 堆操作 + 同时间组列合并）差距更小——N=100 基本持平、
N=1000 约 5.2×——但发散趋势一致：N 越大堆式优势越明显。

## 9. 关键前提与风险

**segment timeRange 必须在文件内按 segPos 单调有序**，否则 watermark 延迟会漏数据：
- 升序：`minT > watermark` 即 stop——前提是后续更高 segPos 的 segment `minT` 只会更大。
- 该前提对降序扩展（§10.1）同样关键：`maxT < watermark` 即 stop 需后续更低 segPos 的 segment
  `maxT` 只会更小。

`ChunkDataBuilder` 有 `timeSorted` 标志，segment 的 `[minT,maxT]` 按 segment 内行计算。openGemini 的
memtable 落盘前按时间排序，因此 ordered/unordered 文件**内部 segment 应当时间有序**（“out-of-order”
是文件间相对概念，非文件内乱序）——但这是分析文档标注“需要进一步验证”的点，且差分测试用手工构造
的 mock 未覆盖。**降序扩展前应先用真实落盘文件 + 多 segment 差分测试验证此前提**。

## 10. 未来优化（待办）

### 10.1 查询路径覆盖矩阵

下表是 openGemini 查询路径中“会读乱序数据”的各路径，及本优化的覆盖情况：

| 路径 | 触发条件 | 乱序读取方式 | 覆盖? | 严重性 |
|---|---|---|---|---|
| TS 非聚合升序 | 默认 | lazy 堆合并 | ✅ | — |
| 降序非聚合 | `!Ascending` | eager 链式 | ❌（§10.2 规划） | 中 |
| limit-cut 非聚合 | `CanLimitCut` | eager 全量读 | ❌（§10.3 规划） | 中 |
| Prom 查询 | `IsPromQuery` | eager（经 tsmMergeCursor） | ❌（未分析） | 中 |
| tsmMergeCursor 聚合 | `len(ops)>0` 且非 fileCursor | pre-agg meta | ❌ | 低 |
| **fileCursor 聚合** | `enableFileCursor`+`HasOptimizeAgg` | **eager 全量 drain** | ❌（§10.4 规划） | 高 |
| 列存 CS/hybrid | `COLUMNSTORE` | 独立 reader | **不考虑** | — |
| 小 N | location 数 < 64 | eager | 设计回退 | — |

- **Prom**：`InstantVectorCursor`/`RangeVectorCursor` → `aggregateCursor` → `seriesCursor` →
  `tsmMergeCursor`。被 `IsPromQuery` 排除，因 Prom 的 lookback/range 窗口语义与 lazy watermark 的
  交互未分析（instant query 的 lookback 跨 ordered batch 边界时，延迟的 unordered 是否破坏采样），
  需专门评估。
- **tsmMergeCursor 聚合**：走 `FirstTimeOutOfOrderInit` → `ReadOutOfOrderMeta` + `AggregateData`，
  用 pre-agg 元数据（不全量读行），内存/性能问题小；first/last/min/max 在时间范围不完全覆盖 chunk
  时退化读 data block。严重性低。
- **列存 CS/hybrid**：`cs_storage.go`/`column_store_reader.go` 是独立 reader，不经 `tsmMergeCursor`，
  乱序合并机制与 TS store 不同。**本优化不考虑 CS/hybrid**，如需覆盖需单独评估。
- **小 N**：阈值回退是设计行为（防退化），非缺口。

### 10.2 降序支持（ORDER BY time DESC）

当前降序走 eager，与升序同病（`MergeRecordDescend` 链式 `O(N²R)` + 全量加载）。降序与升序对称，
把“方向”参数化即可，主要工作：

- **watermark 取有序 batch 的最小时间**：升序 frontier = `MaxTime(true)`（batch max），准入
  `unordered <= watermark`；降序 frontier = `MinTime(false)`（batch min），准入
  `unordered >= watermark`。两者都等于 `orderRecIter.record.lastTime()`（升序 batch 末尾是 max，
  降序 batch 末尾是 min）。已验证 `MergeRecordByMaxTimeOfOldRec` 降序分支边界为
  `oldTimeVals[len-1]`（有序 batch min），与之吻合。
- **`ReadDataBeforeWatermark` 降序分支**：segPos 从高到低遍历（复用 `Location` 的 `!Ascending` 的
  `nextSegment`/`hasNext`）；延迟条件 `if maxT < watermark { return nil }`；过滤用 `FilterByTimeDescend`。
- **merger 堆按时间降序**：`Less` 改 `ti > tj`（时间大先 pop），同时间仍 `seq` 降序；
  `nextBatchUntil` emit 所有 `time >= watermark`；`appendMergedSameTimeRow` 不变。
- **`nextLazy` 降序**：`watermark = record.MinTime(ascending)`；`mergeData(..., ascending=false)`。
- **放开限制**：`lazyUnorderedEnabled()` 去掉 `!Ascending` 判断。
- **预期收益**：与升序同量级（内存 7–58×、`O(N²R)→O(M·logK)`）。降序常取“最近数据”，首个有序
  batch 是最高时间，乱序若多在更早时段则大量被延迟，首批收益尤其明显。
- **前置**：先验证 §9 的 segment 排序前提；补降序差分测试（升序 3000+ 用例方向翻转）。

### 10.3 limit-cut 支持

当前 `lazyUnorderedEnabled()` 对 `CanLimitCut()` 回退 eager。limit-cut 机制（详见
`limit_cut_cursor_analysis.md`）：
- 触发：`HasLimit && !HasCall && !HasFieldCondition && FieldWildcard`，且 `limit+offset < tagSet.Len()`。
- tagset 级：`itrsInitWithLimit` + `topNLinkedList` 按 `limitFirstTime` 砍 series，保留 `limit+offset` 个。
- series 级：`AddLoc` 走 limit-cut 分支——`AddLocationsWithLimit` 按行数裁剪 ordered location；
  `AddLocationsWithFirstTime` 全加 unordered（只读 meta）算 `unorderFirstTime`；
  `limitFirstTime = getFirstTime(orderFirstTime, unorderFirstTime, ascending)`；
  `SetFirstLimitTime` 再与 memtable 首时间取更早者。
- **关键**：`limitFirstTime` 全程只来自 ChunkMeta 元数据（和 memtable 首行），**不依赖读数据**，只在
  init 阶段用于 topN，迭代期间不再使用。limit-cut 不裁剪 unordered 的数据读取（仍全量读）。
- 迭代：与普通路径相同（eager 全量读 unordered + 合并），外层 `limitCursor` 截 `limit+offset`。

**lazy 可兼容 limit-cut 的理由**：`limitFirstTime` 在 `AddLoc`（`FirstTimeInit` 之前）算好，lazy 不
影响；lazy 的 `ensureUnorderedReadyUntil(watermark)` 准入所有 `<= watermark` 的乱序行，首个有序 batch
的 watermark 已覆盖 `limit+offset` 行范围，`limitCursor` 取最早 `limit+offset` 行不会漏。limit-cut 下
ordered 已被裁剪到 ~`limit+offset` 行，单 series 数据量小，lazy 收益有限，但 unordered 全量读的内存
压力仍在（limit-cut 不裁剪 unordered），lazy 仍可降内存。

**放开前需补的差分测试**：ordered 首时间晚于/早于 unordered 首时间、limit 跨多个 ordered batch、
`limitFirstTime` 在 lazy 下与 eager 一致、offset 场景。

### 10.4 fileCursor 聚合路径惰性化方案

**这是覆盖矩阵中严重性最高的未覆盖路径**：`enableFileCursor=true`（默认开）且 `HasOptimizeAgg()`
（pre-agg 可下推的聚合 count/sum/min/max/first/last）时走 `fileLoopCursor`/`agg_tagset_cursor`，
不经 `tsmMergeCursor`。

#### 10.4.1 现状与问题

- **eager 点**：`fileLoopCursor.initMergeIters`（`engine/agg_tagset_cursor.go:411`）在 init 时
  `for i := len(OutOfOrders)-1; i >= 0; i--` 循环，对每个乱序文件建/复用 `fileCursor` 并
  `curCursor.next()` **drain 到 nil**，经 `initOutOfOrderItersByFile` 把全部乱序数据填进
  `mergeRecIters[sid][idx]`（per-sid 的 `SeriesIter` 列表，含 memtable + 全部乱序）。
- **消费模型**：`ReadAggDataNormal` 按时间序遍历 ordered 文件（`getFile()`），每个 ordered 文件建
  `fileCursor` 读该文件各 sid 的数据，与 `mergeRecIters[sid][idx]` 合并/聚合；**某 sid 的乱序在其出现
  的首个 ordered 文件处被消费一次并 `delete(mergeRecIters, sid)`**（`file_cursor.go:247`）。ordered
  耗尽后 `ReadAggDataOnlyInMemTable` drain 剩余 `mergeRecIters`（不在任何 ordered 文件中的 sid）。
- **成本分两档**（`fileCursor.isPreAgg = ctx.decs.MatchPreAgg()`）：
  - `readPreAggData`（`MatchPreAgg`）：读 pre-agg **元数据**，eager 全量加载的是 meta，成本较低。
  - `readData`（`!MatchPreAgg` 但 `HasOptimizeCall`，退化 pre-agg）：读**全量数据行**，eager 全量加载
    是内存/GC + 重复读的主要问题，与 `tsmMergeCursor` 旧路径同构。

#### 10.4.2 为什么不能照搬 tsmMergeCursor 的 segment 级延迟

`fileLoopCursor` 的“**消费一次**”模型与 `tsmMergeCursor` 的流式合并根本不同：某 sid 的乱序只在
其首个 ordered 文件处被消费并删除。若按 watermark 文件级延迟加载乱序，sid S 在首个 ordered 文件 F1
（watermark=F1.maxT）处只消费了 `minT <= F1.maxT` 的乱序并被删除，后续延迟到 F2 才加载的 S 的乱序
就**孤立丢失**。因此简单文件级延迟会破坏正确性。

#### 10.4.3 方案：重构为 per-sid 流式 watermark 合并

把“init 全量加载 + 首个 ordered 文件消费一次”改为“按 ordered watermark 增量加载、跨 ordered 文件
流式合并”，本质是给 `fileLoopCursor` 引入类似 `tsmMergeCursor` 的 lazy merger，但作用于 per-sid 的
`mergeRecIters`：

1. **乱序文件按时间排序**：`OutOfOrders` 按 `file.MinMaxTime()` 的 minT 排序（升序查询升序排），
   维护游标 `unorderFileIdx`。
2. **去掉 `initMergeIters` 的全量 drain**：仅加载 memtable（`initMemitrs`）+ 第一个 ordered 文件
   watermark 内的乱序文件。
3. **watermark 推进时增量加载**：`ReadAggDataNormal` 每推进到一个 ordered 文件 F（watermark=F.maxT
   升序 / F.minT 降序），把 `minT <= watermark` 且未加载的乱序文件 drain 进 `mergeRecIters`；超过
   watermark 的延迟。
4. **改变消费模型**：sid S 不再在首个 ordered 文件处删除，而是跨多个 ordered 文件流式合并——每
   个含 S 的 ordered 文件 F_k 与 S 在 `[F_k.minT, F_k.maxT]` 内的乱序合并，S 的 `mergeRecIters`
   随 watermark 增长，直到 S 的所有乱序读完。这要求 `fileCursor` 的合并/聚合改为“按当前 ordered
   文件时间窗口切片”而非“一次性全合并”。
5. **ordered 耗尽后**：drain 剩余延迟的乱序文件（`ReadAggDataOnlyInMemTable` 扩展为按 sid 流式输出）。
6. **`SetLastFile` 语义调整**：当前 `isLastFile` 标记最后一个乱序文件用于 memdata fallback
   （`file_cursor.go:304`、`agg_tagset_cursor.go:468`）；增量加载下需在“最终 drain 阶段”才置
   `isLastFile`，不能在 init 时固定。

#### 10.4.4 正确性要点

- **pre-agg 聚合的结合律**：sum/count/min/max 的 pre-agg meta 可跨任意时间范围增量聚合（结合律），
  first/last 的 pre-agg meta 携带时间戳参与比较——因此增量加载+增量聚合与一次性全聚合**等价**。
  需用差分测试验证（eager 全量聚合 vs lazy 增量聚合，对比最终聚合结果）。
- **readData（非 pre-agg）的时间序**：`mergeData` 是时间序合并，lazy 按 watermark 切片与 eager
  全量合并等价（同 `tsmMergeCursor` 的不变式）。
- **不漏**：watermark 推进前必须准入所有 `<= watermark` 的乱序；文件级 `minT <= watermark` 判定
  保证不漏（`minT > watermark` 的文件全部数据 `> watermark`，延迟安全；重叠文件 `minT <= watermark
  < maxT` 整文件加载会 over-read `> watermark` 部分，由后续 ordered 文件消费，正确）。

#### 10.4.5 落地建议与风险

- **范围**：先做 `readData`（非 pre-agg）子路径——它与 `tsmMergeCursor` lazy 同构，收益最大、风险
  可控；`readPreAggData` 子路径因 meta 成本低，优先级低，可后做。
- **风险**：`fileLoopCursor` 的 per-sid 消费模型重构面较大（`mergeRecIters` 生命周期、`SetLastFile`、
  `ReadAggDataOnlyInMemTable`）；比 `tsmMergeCursor` 的 lazy 改动复杂得多。
- **前置**：先量化 `readData` 模式 fileCursor 查询在生产的出现频率（`!MatchPreAgg && HasOptimizeCall`）；
  若罕见，则该路径优先级降低，聚焦 §10.2/§10.3。
- **测试**：fileCursor 路径需独立的差分测试设施（mock 多 ordered/乱序文件 + pre-agg meta），目前
  测试基础设施不足，需先建。

### 10.5 Phase 2：schema/field 预过滤
`AddLocations` 里在 `Contains` 命中后，若 ChunkMeta 的列与查询 schema 无交集则不加入 location。分析
文档指出 count(time)/aux/Prom 语义需进一步验证；惰性路径已按 watermark 跳过未来 segment，边际收益
变小。风险：错跳 needed location → 结果错。需保守条件 + 专门测试。

### 10.6 Phase 4：后台分层合并
乱序文件过多的 shard/measurement 触发后台预合并/分层 compact，从源头降低 N。长期 compaction 侧工作，
超出查询路径范围。

### 10.7 全重叠 fallback
所有乱序与有序同时间时，惰性无延迟收益，且 per-row `AppendColVal` 合并慢于 eager 向量化
`MergeRecord`，堆中需持有全部 K 个源 → 时间与内存均无优势。非典型人造最坏情况；待办：重叠感知
fallback（lazy 路径发现首批即准入大部分乱序时切回 eager）。

### 10.8 其他
- **config 接入**：`lazyUnorderedMergeEnabled` 目前仅运行时 setter（同 `EnableFileCursor`），未接
  `Query` 配置项。
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
| `limit_cut_cursor_analysis.md` | limit-cut 机制详细分析（本文 §10.3 摘要） |
| `unordered_merge_e2e_perf_plan.md` | 端到端性能压测方案（数据模型/写入/查询全链路，对照理论曲线） |

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
