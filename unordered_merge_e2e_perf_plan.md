# 乱序合并优化：端到端性能压测方案

本方案基于当前优化方案（堆式 K 路合并 + `maxRowCnt` 流式分批，升序降序均支持）设计，目标是在真实
集群上端到端验证优化收益，覆盖**数据模型、写入、查询**全链路。仅设计方案，不含编码。

## 1. 目标与范围

**主目标**（非首包延迟——查询通常迭代完所有数据才返回）：
1. **总查询性能**：乱序文件数 N 较大时，优化路径总查询耗时显著低于 eager；随 N 增长，eager 耗时
   ~`N²`、优化路径 ~`N·logN`（复现理论曲线的发散趋势）。
2. **内存/GC 压力**：优化路径分配量（B/op）显著低于 eager（`O(M)` vs `O(N²R)`）；峰值 live 内存
   多段文件下降低（堆持 K 段 << eager 的全部行 `outRec`）。

**回归目标**：
3. 小 N（< 阈值 64）不退化（阈值路由到 eager）。
4. 不支持形状（聚合/limit-cut/Prom）回退 eager，无性能或结果差异。
5. **升序与降序**均正确且性能收益一致（优化路径支持双向）。

**不在范围**：列存 CS/hybrid（独立 reader，不考虑）。

## 2. 测试环境

- **部署**：单机 `ts-server`（standalone），避免集群噪声。
- **硬件**：固定一台机器，记录 CPU/内存/磁盘（SSD 推荐）。
- **数据目录**：每次测试前清空 `data-dir`。
- **监控**：`ts-monitor` + `pprof`（CPU/heap/block）+ `iostat`。

### 2.1 关键配置（控制 N、R，冻结 compaction）

| 配置项 | 取值 | 作用 |
|---|---|---|
| `[memtable] shard-mutable-size-limit` | 小（1MB） | 每批次写入即 flush，精确产生 N 个乱序文件 |
| `[memtable] write-cold-duration` | 短（1s） | 加速 flush |
| `[data] max-unordered-file-number` | 大（2000） | 抑制乱序合并，冻结 N |
| `[compaction] max-concurrent-compactions` | 0 | 测量期间关闭 compaction |
| `[data] max-rows-per-segment` | 可调 | 控制 R（每 segment 行数） |

## 3. 数据模型与写入设计

### 3.1 数据布局（真实 disjoint）

openGemini 的 `SplitRecordByTime` 在 flush 时按已落盘有序最大时间 `flushTime` 切分：
- `time > flushTime` → ordered（更新）
- `time <= flushTime` → unordered（更旧）

二者时间不相交。造数时严格遵循此布局。

### 3.2 数据模型

- **measurement**：`mst`，F 个整型字段 `f0..f{F-1}`。
- **tag**：`k0`，S 个 series。
- schema 固定不变。

### 3.3 写入阶段

1. **有序区**（先写）：S 个 series，升序写入 `[T_order_start, T_order_end]`（新时间段）。落盘为
   ordered 文件。
2. **乱序区**（后写）：N 个批次，每批次 S 个 series × R 行，时间戳在 `[T_unorder_start,
   T_unorder_end]`（早于有序区，`T_unorder_end < T_order_start`）。每批次写入量 >
   `shard-mutable-size-limit` → flush 为 1 个乱序文件。
3. **乱序文件间重叠度**（验证 §8.2 的 Overlap/NonOverlap 分支影响）：
   - `disjoint`：各乱序文件时间范围互不重叠（eager NonOverlap 分支，低常数）。
   - `overlapping`：各乱序文件时间范围重叠（eager Overlap 分支，高常数；优化路径同时间组 churn）。
4. **造数后等待**：flush 完成（`write-cold-duration` × 2），确认 unordered 文件数 == N。

### 3.4 变量取值

| 变量 | 含义 | 取值 |
|---|---|---|
| N | 乱序文件数 | 1, 10, 32, 64, 100, 200, 500, 1000 |
| R | 每文件每 series 行数 | 20, 100, 1000 |
| S | series 数 | 1, 100, 10000 |
| F | 字段数 | 1, 5, 20 |
| 乱序文件间重叠 | disjoint / overlapping | 2 组 |
| 查询方向 | 升序 / 降序 | 2 组 |

> 优先固定 R=100、S=100、F=5、disjoint，扫 N 全梯度 × 2 方向；再对 N=1000 做多维交叉。

## 4. 查询负载

| 查询类型 | SQL | 优化路径覆盖 | 说明 |
|---|---|---|---|
| **升序非聚合** | `SELECT * FROM mst WHERE time >= ... AND time <= ...` | ✅ | 主查询，Phase 1 乱序流式 → Phase 2 有序 |
| **降序非聚合** | `... ORDER BY time DESC` | ✅ | Phase 1 有序 → Phase 2 乱序流式；堆 max-heap |
| **聚合** | `SELECT count(f0), sum(f1) FROM mst ...` | ❌ 回退 eager | `len(ops)>0` |
| **limit-cut** | `SELECT * FROM mst ... LIMIT n` | ❌ 回退 eager | `CanLimitCut` |
| **Prom** | PromQL 查询 | ❌ 回退 eager | `IsPromQuery` |

每查询迭代至 cursor 耗尽（总耗时是主指标）。

## 5. 测试矩阵

### 5.1 主轴：N 扫描 × 方向（复现理论曲线）

固定 R=100、S=100、F=5、disjoint。
N ∈ {1,10,32,64,100,200,500,1000} × {升序, 降序}，flag on/off 各 ≥20 次。

**预期**：eager 耗时 ~`N²`、优化路径 ~`N·logN`；升序降序趋势一致（底层数据扫描相同，仅 Phase 顺序
+ 堆方向不同）。绘制实测耗时-N 曲线，对照 `unordered_merge_complexity_chart`。

### 5.2 多维交叉（N=1000 固定）

- **R** ∈ {20,100,1000}：R 越大 eager `O(N²R)` 放大越明显。
- **S** ∈ {1,100,10000}：series 数对 AddLoc + 合并的叠加。
- **F** ∈ {1,5,20}：字段数对 decode/merge 列拷贝。
- **乱序文件间重叠** ∈ {disjoint, overlapping}：验证 Overlap/NonOverlap 分支影响。
- **方向** ∈ {升序, 降序}。

### 5.3 多段文件峰值验证

真实 TSSP 文件是多段的（`max-rows-per-segment` 控制）。构造 S=10 段/文件、R=20 行/段、N=100，
M=20000 总行：
- 验证优化路径峰值 live 内存（堆持 K 段 = M/S）< eager（`outRec` = M）。
- 用 `pprof` heap 采样 + 激进 GC（`GOGC=1`）测 live 峰值。
- 对照微基准数据：多段 8× 更快、56× 省分配。

### 5.4 回归矩阵

N=1000，flag on/off × {聚合, limit-cut, Prom}。**预期**：flag on 时走 eager，结果与 flag off 完全
一致、耗时差异 <10%。

### 5.5 阈值验证

N ∈ {10,32,64,100}，flag on：确认 N<64 走 eager（耗时与 flag off 一致）、N≥64 走优化路径。
通过 span 计数 `unordered_location_count` + 耗时拐点验证。

## 6. 指标采集

### 6.1 查询侧
- **总耗时**（主指标）：p50/p95/p99，迭代至完成。
- 首行耗时（参考）。
- 返回行数（校验正确性，flag on/off 必须一致）。
- **升序与降序结果一致性**：flag on 升序结果 == flag off 升序结果；降序同理。

### 6.2 资源侧
- **分配量**（B/op 代理）：`runtime.ReadMemStats.TotalAlloc` delta；优化路径 `O(M)` vs eager
  `O(N²R)`。
- **峰值 live 内存**：`pprof` heap 采样 + 激进 GC（`GOGC=1`）使 HeapInuse 逼近 live；多段文件下
  优化路径应低于 eager。
- **GC**：GC 次数/暂停时间。
- **CPU profile**：确认 `MergeRecord`/`mergeRecRow`（eager）占比下降、堆操作（优化路径）占比。
- **磁盘 I/O**：`iostat` 读吞吐/IOPS。

### 6.3 引擎内 span 计数
`unordered_location_count`、`unordered_merge_count`、`unorder_duration`、`tsmIterDuration`——
核对 N、合并次数、乱序读取耗时。

## 7. 方法论

1. **造数一次**：每个 (N,R,S,F,重叠度) 组合造数一次，flag on/off 共用。
2. **清缓存**：flag 切换间重启 ts-server 或 `drop_caches`。
3. **预热**：每组正式测量前跑 3-5 次预热，丢弃。
4. **隔离**：测量期间无其他查询/写入/compaction。
5. **重复**：每组 ≥20 次，取 p50/p95/p99。
6. **正确性校验**：flag on/off 返回行数与结果集逐行一致；升序降序各自独立校验。
7. **对照理论**：实测"eager/优化路径"比值随 N 的曲线，叠加到 `unordered_merge_complexity_chart`
   理论比值上，看发散趋势是否一致。

## 8. 预期结果与判定

| 场景 | 预期（优化路径 vs eager） | 判定 |
|---|---|---|
| N=1000, disjoint, 升序 | 总耗时 ↓ ~5×；分配量 ↓ ~64× | 通过 |
| N=1000, disjoint, 降序 | 同升序（底层数据扫描相同） | 通过 |
| N=100, disjoint | 总耗时 ~持平；分配量 ↓ ~8× | 通过 |
| N<64 | 走 eager（阈值），耗时与 flag off 一致 | 通过（无退化） |
| N=1000, overlapping | 优化路径仍有 CPU 收益（堆 vs 链式）；同时间组 churn 常数升高 | 可接受 |
| 多段文件 (S=10) | 峰值 live 内存 ↓；总耗时 8× 更快 | 通过 |
| 聚合/limit-cut/Prom (N=1000) | flag on 与 off 结果一致、耗时差异 <10% | 通过（回退正确） |
| N 扫描曲线 | eager~N²、优化路径~N·logN 发散趋势可见 | 通过 |

**失败判据**：
- 任一 flag on 场景结果集与 flag off 不一致（正确性）。
- 小 N 场景耗时退化 >20%（阈值失效）。
- 回归形状耗时退化 >20%。
- 升序与降序性能收益差异显著（应一致，因底层数据扫描相同）。

## 9. 风险与缓解

- **compaction 干扰 N**：`max-concurrent-compactions=0` + `max-unordered-file-number=2000`；每轮
  前后核对文件数。
- **memtable 残留**：造数后等待 `write-cold-duration×2`，确认 memtable 为空。
- **文件缓存噪声**：重启或 `drop_caches`；报告是否清缓存。
- **N=1000 造数耗时**：用批量 Line Protocol 写入。
- **峰值内存测量精度**：HeapInuse 被分配量主导，需激进 GC（`GOGC=1`）逼近 live；微基准已验证此
  方法可区分多段 vs 单段。
- **降序 mock 验证**：微基准中 mock `ReadAt` 已修复降序反转（匹配真实 `reverseValues`）；端到端用
  真实文件不需此修复，但确认降序结果正确。

## 10. 交付物

1. 造数脚本规格：按 (N,R,S,F,重叠度) 生成 Line Protocol 批量写入，控制 flush 与乱序时间戳。
2. 查询 runner 规格：按矩阵跑查询、采 pprof/iostat、记录 span 计数、校验结果一致性与升序降序
   各自正确性。
3. 指标报告模板：每组 (条件, flag, 方向, 重复次数) → p50/p95/p99 耗时、分配量、峰值堆、GC、I/O、
   span 计数；附 N-耗时曲线与理论曲线对照图。
4. 结论：是否达成 §1 两个主目标 + 回归目标；优化路径放开的推荐阈值与适用场景。

## 11. 与微基准的对应

| 微基准结果 | 端到端验证点 |
|---|---|
| Total N=1000：5.4× 更快、64× 省分配 | 真实 I/O 下总耗时 + GC 压力 |
| FirstPacket N=1000：28× 更快 | 流式分批首包（参考，非主指标） |
| MultiSeg：8× 更快、56× 省分配 | 多段真实文件（主场景） |
| 升序 = 降序（布局无关） | 升序降序收益一致 |
| 阈值 64：N<64 走 eager 不退化 | 端到端阈值验证 |
| Overlap/NonOverlap 分支影响 | disjoint vs overlapping 数据集 |
