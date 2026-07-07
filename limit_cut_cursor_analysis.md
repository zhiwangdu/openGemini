# limit-cut 场景下 tsmMergeCursor 的初始化与迭代分析

本文档分析 openGemini 查询路径中 limit-cut（`CanLimitCut`）场景下 cursor 的初始化与迭代流程，
以及与本仓库“乱序惰性合并优化”的交互关系。源码位置以当前分支为准。

## 1. 何时触发 limit-cut

`CanLimitCut()`（`engine/executor/schema.go:575`）：
```go
return qs.HasLimit() && !qs.HasCall() && !qs.HasFieldCondition() && qs.Options().FieldWildcard()
```
即：有 LIMIT、无聚合 call、无字段过滤条件、且字段是 wildcard（`SELECT *`）。

再加一个 tagset 级门槛（`engine/iterators.go:763` / `:801`）：
```go
schema.CanLimitCut() && schema.Options().GetLimit()+schema.Options().GetOffset() < tagSet.Len()
```
即 series 数多于 `limit+offset` 才值得 cut。

## 2. 初始化：两层“cut”

### 2a. tagset 级：top-N 选 series（`itrsInitWithLimit`）

`itrsInitWithLimit`（`engine/iterators.go:1003`）遍历 tagset 里每个 series，建 `seriesCursor`，
**全部插入** `topNLinkedList`（`engine/topn_linkedlist.go`），链表容量 = `limit+offset`，按每个 series
的 `limitFirstTime` 升/降序保留前 N 个 series，**其余 series 直接丢弃**（不进入 heap，不迭代）。
保留下来的每个 series 包一层 `limitCursor`（`engine/iterators.go:1075` 附近）。

所以 limit-cut 的核心是：**用每个 series 的“可能最早时间”做 topN，砍掉不可能进 limit 的 series**。

### 2b. series 级：`limitFirstTime` 怎么算

建 `tsmMergeCursor` 时 `AddLoc()`（`engine/tsm_merge_cursor.go:366`）走 limit-cut 分支：

- **ordered** → `AddLocationsWithLimit`（`engine/tsm_merge_cursor.go:215`）：按时间方向遍历 ordered
  文件，`loc.Contains` 命中就 `AddLocation`，并用 `ChunkMeta.TimeMeta().RowCount` 累计行数 `orderRow`；
  **一旦 `orderRow >= limit+offset` 就 break**。所以 ordered 只加入“够满足 limit+offset 行”的 location
  （ordered 文件级裁剪）。同时记录 `orderFirstTime`（首个命中 location 的 meta min/max time）。
- **unordered** → `AddLocationsWithFirstTime`（`engine/tsm_merge_cursor.go:273`）：遍历**所有**
  out-of-order 文件，命中的全部 `AddLocation`（**不裁剪**），并从每个 ChunkMeta 的 MinMaxTime 算出
  `unorderFirstTime`（升序取 min，降序取 max）。**只读 meta，不读数据**。
- `limitFirstTime = getFirstTime(orderFirstTime, unorderFirstTime, ascending)`
  （`engine/iterators_helper.go:120`）：取 ordered/unordered 两者中更“早”的那个（0/-1 表示无数据）。

然后 `seriesCursor.SetFirstLimitTime`（`engine/series_cursor.go:136`）把 `tsmCursor.limitFirstTime`
和 memtable 首行时间再取更早者，作为该 series 的 `limitFirstTime`，供 topN 比较用。

> 关键：`limitFirstTime` 全程只来自 **ChunkMeta 元数据**（和 memtable 首行），**不依赖读数据**。
> 它只在 init 阶段用于 topN 选 series，迭代期间不再使用。

## 3. 迭代：与普通路径基本相同

limit-cut **不改单 series 的迭代方式**，仍是 eager `FirstTimeInit`：
- `FirstTimeInit` 把所有命中的 unordered location（`AddLocationsWithFirstTime` 全部加入的那些）读空、
  链式合并成 `outRec`。
- `Next()` 读 ordered（已被 `AddLocationsWithLimit` 裁剪过，只读够 limit+offset 行的 ordered 文件），
  `mergeData` 合并 ordered + outRec。
- `tagSetCursor` heap 归并各 series，`limitCursor` 在外层把输出截到 `limit+offset` 行（再扔掉前 offset 行）。

所以 limit-cut 的收益来自：① ordered 文件级裁剪（少读 ordered）；② topN 砍 series（少迭代 series）。
**unordered 数据仍是全量读、全量合并**（eager），limit-cut 并不裁剪 unordered 的读取。

## 4. 与本优化的交互（为何 lazy 路径对 limit-cut 回退）

“乱序惰性合并优化”的 `lazyUnorderedEnabled()`（`engine/tsm_merge_cursor.go`）里
`if c.ctx.querySchema.CanLimitCut() { return false }` —— limit-cut 场景回退 eager。

原因不是 `limitFirstTime`（那是 meta 算的，lazy 不影响），而是：
- limit-cut 下 ordered 已被 `AddLocationsWithLimit` 裁剪到 ~limit+offset 行，单 series 数据量本就不大，
  lazy 的 watermark 延迟收益有限；
- limit-cut + lazy 的组合没有差分测试覆盖，而 limit/offset 对“不能漏更早乱序行”极敏感，保守回退更安全。

理论上 lazy 其实**可以**兼容 limit-cut：
- `limitFirstTime` 在 `AddLoc`（`FirstTimeInit` 之前）就算好了，lazy 不影响；
- lazy 的 `ensureUnorderedReadyUntil(watermark)` 会准入所有 `<= watermark` 的乱序行，而首个 ordered batch
  的 watermark 已覆盖 limit+offset 行的范围，`limitCursor` 取最早 limit+offset 行，不会漏。

但要安全放开，需要补 limit-cut + lazy 的差分测试，覆盖：
- ordered 首时间晚于 unordered 首时间；
- ordered 首时间早于 unordered 首时间；
- limit 跨多个 ordered batch；
- `limitFirstTime` 在 lazy 路径下与 eager 一致；
- offset 场景。

## 5. 小结

| 阶段 | limit-cut 做了什么 |
|---|---|
| tagset init | topN 按 `limitFirstTime` 砍 series，保留 limit+offset 个 |
| series AddLoc | ordered 按 limit+offset 行裁剪 location；unordered 全加（只读 meta 算 firstTime） |
| limitFirstTime | = min/max(ordered 首时间, unordered 首时间, memtable 首时间)，仅用于 topN |
| 迭代 | 与普通路径相同（eager 全量读 unordered + 合并），外层 `limitCursor` 截 limit+offset |
| 本优化 | limit-cut 场景回退 eager（保守，缺测试覆盖）；理论上可兼容，需补差分测试后放开 |

## 6. 关键源码位置

| 文件 | 内容 |
|---|---|
| `engine/executor/schema.go:575` | `CanLimitCut()` |
| `engine/iterators.go:763,801` | tagset 级 limit-cut 门槛、`doLimitCut` |
| `engine/iterators.go:1003` | `itrsInitWithLimit`（topN 选 series） |
| `engine/topn_linkedlist.go` | `topNLinkedList`（按 limitFirstTime 排序保留前 N） |
| `engine/tsm_merge_cursor.go:215` | `AddLocationsWithLimit`（ordered 裁剪） |
| `engine/tsm_merge_cursor.go:273` | `AddLocationsWithFirstTime`（unordered 全加 + 算 firstTime） |
| `engine/tsm_merge_cursor.go:366` | `AddLoc()` limit-cut 分支 |
| `engine/iterators_helper.go:120` | `getFirstTime`（合并 ordered/unordered 首时间） |
| `engine/series_cursor.go:136` | `SetFirstLimitTime`（series 级，含 memtable） |
