# 乱序合并优化：端到端性能压测方案

本方案基于理论复杂度对比（链式 `O(N²·R)` vs 堆式 `O(M·logK)`，见
`unordered_merge_complexity_chart`）设计，目标是在真实集群上端到端验证优化收益，覆盖
**数据模型、写入、查询**全链路。仅设计方案，不含编码。

## 1. 目标与范围

**主目标**（对应优化的两个原始目标，非首包延迟）：
1. **总查询性能**：乱序文件数 N 较大时，lazy 路径总查询耗时显著低于 eager；随 N 增长，eager
   耗时 ~`N²`、lazy ~`N·logN`（复现理论曲线的发散趋势）。
2. **内存/GC**：lazy 峰值堆内存显著低于 eager（eager 的 `outRec` = 全部乱序）。

**回归目标**：
3. 小 N（< 阈值 64）不退化（阈值应路由到 eager）。
4. 不支持形状（降序/聚合/limit-cut/Prom）回退 eager，无性能或结果差异。

**不在范围**：列存 CS/hybrid（设计文档 §10.1 已标“不考虑”）。

## 2. 测试环境

- **部署**：单机 `ts-server`（standalone，`config/openGemini.singlenode.conf`），避免集群分布式
  噪声；如需验证并发，另起一组多 ts-sql/ts-store 集群测试。
- **硬件**：固定一台机器，记录 CPU 核数/主频、内存、磁盘类型（SSD/HDD）、可用空间。磁盘建议 SSD
  以隔离 I/O 差异（若验证 I/O 收益可单独跑 HDD 组）。
- **数据目录**：每次完整测试前清空 `/tmp/openGemini`（或 `data-dir`），避免历史数据干扰。
- **监控**：`ts-monitor` + `pprof`（CPU/heap/block）+ `iostat`/`vmstat`。

### 2.1 关键配置（控制 N、R，冻结 compaction）

| 配置项 | 取值 | 作用 |
|---|---|---|
| `[memtable] shard-mutable-size-limit` | 小（如 1MB） | 每次 unordered 批次写入即触发 flush，便于精确产生 N 个乱序文件 |
| `[memtable] write-cold-duration` | 短（如 1s） | 加速 memtable 变冷 flush |
| `[data] max-unordered-file-number` | 大（如 2000） | 抑制乱序文件合并，冻结 N |
| `[data] max-unordered-file-size` | 大（如 64GB） | 同上 |
| `[compaction] max-concurrent-compactions` | 0 | 测量期间完全关闭 compaction，保证 N 不变 |
| `[data] max-rows-per-segment` | 可调 | 控制 R（每 segment 行数） |
| `[data] unordered-only` | true（仅造数阶段可选） | 全部按乱序处理，便于构造纯乱序场景 |

> 测量结束后恢复生产配置。关闭 compaction 仅用于冻结 N 以便对照理论曲线。

## 3. 数据模型与写入设计

核心是**可控地产生 N 个乱序文件**，并独立控制 R、S、F、重叠度。

### 3.1 数据模型

- **measurement**：`mst`，F 个整型字段 `f0..f{F-1}`。
- **tag**：`k0`（区分 series）。S 个 series（S 个 `k0` 值）。
- schema 固定后不再变更（避免 schema 演进干扰）。

### 3.2 写入阶段（造数）

1. **有序区**（先写）：S 个 series，时间戳升序写入 `T_ordered` 个点（如每 series 1000 点，时间
   `[t0, t0+1000s)`）。这部分落盘为 ordered 文件。
2. **乱序区**（后写）：写 N 个批次，每批次：
   - 每个 series 写 R 行，时间戳在 `[t0 - Δ, t0 - Δ + R)` 范围（早于有序区，制造 out-of-order）；
   - 每批次时间范围不同（互不重叠或部分重叠），Δ 随批次递增；
   - 每批次写入量略大于 `shard-mutable-size-limit`，触发一次 flush → 1 个乱序文件。
3. **重叠度控制**（三组数据集，见 §5 矩阵）：
   - `no-overlap`：乱序时间戳全部早于有序区最小时间（lazy 可大量延迟）。
   - `partial-overlap`：乱序时间戳与有序区部分重叠。
   - `full-overlap`：乱序时间戳落在有序区范围内（lazy 无延迟收益，验证 §10.7 fallback 必要性）。
4. **造数后等待**：所有 memtable flush完成（`write-cold-duration` × 2 + 余量），用 `ts-cli` 或文件
   目录确认 unordered 文件数 == N（`ls data/.../out-of-order/ | wc -l` 或 `ts-cli` 统计）。

### 3.3 变量取值（与理论曲线对齐）

| 变量 | 含义 | 取值 |
|---|---|---|
| N | 乱序文件数 | 1, 10, 32, 64, 100, 200, 500, 1000（覆盖阈值两侧 + 理论曲线关键点） |
| R | 每文件每 series 行数 | 20, 100, 1000 |
| S | series 数 | 1, 100, 10000 |
| F | 字段数 | 1, 5, 20 |
| overlap | 重叠度 | no / partial / full |

> 优先固定 R=100、S=100、F=5，扫 N 全梯度（复现理论曲线）；再对 N=1000 做多维交叉。

## 4. 查询负载

- **主查询（lazy 覆盖）**：非聚合升序全范围扫描
  `SELECT * FROM mst WHERE time >= '<start>' AND time <= '<end>'`（默认升序），迭代至完成。
- **回归查询（eager 回退）**：
  - 降序：`... ORDER BY time DESC`（验证 §10.2 未放开前回退 eager）。
  - 聚合：`SELECT count(f0), sum(f1) FROM mst ...`（fileCursor 路径，验证 §10.4 未优化前无变化）。
  - limit-cut：`SELECT * FROM mst ... LIMIT n`（`CanLimitCut`，验证 §10.3 未放开前回退）。
- 每查询迭代至 cursor 耗尽（总耗时是主指标，非首包）。

## 5. 测试矩阵

### 5.1 主轴：N 扫描（复现理论曲线）
固定 R=100、S=100、F=5、overlap=no-overlap、主查询（升序非聚合）。
N ∈ {1,10,32,64,100,200,500,1000}，每组 flag on/off 各跑 ≥20 次。
**预期**：eager 耗时随 N ~二次增长，lazy ~`N·logN`；绘制实测耗时-N 曲线，与
`unordered_merge_complexity_chart` 的理论发散趋势对照。

### 5.2 多维交叉（N=1000 固定，验证各维度影响）
- R ∈ {20,100,1000}：R 越大链式 `O(N²R)` 放大越明显，lazy 优势应更突出。
- S ∈ {1,100,10000}：series 数对 AddLoc 放大与合并的叠加影响。
- F ∈ {1,5,20}：字段数对 decode/merge 列拷贝的影响。
- overlap ∈ {no, partial, full}：full-overlap 应出现 lazy 无收益/略差（验证 fallback）。

### 5.3 回归矩阵
N=1000，flag on/off × {降序, 聚合, limit-cut}。**预期**：flag on 时这些形状走 eager，结果与
flag off 完全一致、耗时无显著差异（验证回退正确 + 无额外开销）。

### 5.4 阈值验证
N ∈ {10,32,64,100}，flag on：确认 N<64 时走 eager（耗时与 flag off 一致）、N≥64 走 lazy。
通过 span 计数 `unordered_location_count` + 耗时拐点验证阈值生效。

## 6. 指标采集

### 6.1 查询侧
- **总耗时**（主指标）：p50/p95/p99，迭代至完成。
- 首行耗时（参考，非主）。
- 返回行数（校验正确性，flag on/off 必须一致）。

### 6.2 资源侧
- **峰值堆内存**：`curl /debug/pprof/heap` 采样 + 进程 RSS；eager 应出现 `outRec` 大峰值，lazy
  应显著更低（理论 N=1000 时 ~58×）。
- **GC**：`GOGC` 默认，记录 GC 次数/暂停（`/debug/pprof` + runtime stats）。
- **CPU profile**：确认链式路径 `MergeRecord`/`mergeRecRow` 占比下降、堆路径 `heap` 操作占比。
- **磁盘 I/O**：`iostat` 读吞吐/ IOPS；lazy 应在 no-overlap 下读取量更小（延迟了未来 segment）。

### 6.3 引擎内 span 计数（已实现）
`unordered_location_count`、`unordered_merge_count`、`unorder_duration`、`tsmIterDuration`——
用于核对 N、合并次数、乱序读取耗时，与理论预期对照。

## 7. 方法论

1. **造数一次**：每个 (N,R,S,F,overlap) 组合造数一次，flag on/off 共用同一数据集。
2. **清缓存**：flag 切换间重启 ts-server 或 `echo 3 > /proc/sys/vm/drop_caches`，消除文件缓存影响
   （若测 I/O）；若仅测 CPU/内存可保留缓存以隔离。
3. **预热**：每组正式测量前跑 3-5 次预热查询，丢弃。
4. **隔离**：测量期间无其他查询、无写入、无 compaction（已关）。
5. **重复**：每组 ≥20 次，取 p50/p95/p99，报告均值±置信区间。
6. **正确性校验**：flag on/off 的返回行数与结果集必须逐行一致（端到端 oracle）。
7. **对照理论**：把实测“eager 耗时 / lazy 耗时”比值随 N 的曲线，叠加到
   `unordered_merge_complexity_chart` 的理论比值曲线上，看趋势是否一致（实测比值应小于理论，
   因常数因子，但发散趋势一致）。

## 8. 预期结果与判定

| 场景 | 预期（lazy vs eager） | 判定 |
|---|---|---|
| N=1000, no-overlap, 升序非聚合 | 总耗时 ↓ ~5×（理论 100×，实测受常数因子影响）；峰值堆 ↓ ~50× | 通过 |
| N=100, no-overlap | 总耗时 ~持平；峰值堆 ↓ ~7× | 通过 |
| N=10 / N=32 | 走 eager（阈值），耗时与 flag off 一致 | 通过（无退化） |
| N=1000, full-overlap | lazy 无收益或略慢（≤1.5×）；内存相近 | 可接受，记录为 §10.7 fallback 依据 |
| 降序/聚合/limit-cut (N=1000) | flag on 与 off 结果一致、耗时差异 <10% | 通过（回退正确） |
| N 扫描曲线 | eager~N²、lazy~N·logN 发散趋势可见 | 通过 |

**失败判据**：任一 flag on 场景结果集与 flag off 不一致（正确性）；或小 N 场景耗时退化 >20%（阈值
失效）；或回归形状耗时退化 >20%。

## 9. 风险与缓解

- **compaction 干扰 N**：`max-concurrent-compactions=0` + `max-unordered-file-number=2000`；每轮
  前后核对文件数。
- **memtable 残留**：造数后等待 `write-cold-duration×2`，并确认 memtable 为空。
- **文件缓存噪声**：重启或 drop_caches；报告是否清缓存。
- **N=1000 造数耗时**：可接受较长时间造数（一次性）；用批量写入工具（openGemini-write 或
  Line Protocol 批量接口）。
- **测量噪声**：≥20 次重复 + p95；固定硬件、关闭无关进程。
- **阈值边界**：N=64 附近多取几个点（60,64,68）确认拐点位置。

## 10. 交付物

1. 造数脚本规格：按 (N,R,S,F,overlap) 生成 Line Protocol 批量写入，控制 flush 与乱序时间戳。
2. 查询 runner 规格：按矩阵跑查询、采 pprof/iostat、记录 span 计数、校验结果一致性。
3. 指标报告模板：每组 (条件, flag, 重复次数) → p50/p95/p99 耗时、峰值堆、GC、I/O、span 计数；
   附 N-耗时曲线与理论曲线对照图。
4. 结论：是否达成 §1 两个主目标 + 回归目标；lazy 放开的推荐阈值与适用场景。

## 11. 与理论/已有工作的对应

- 理论曲线：`unordered_merge_complexity_chart`（链式 `O(N²R)` vs 堆式 `O(M·logK)`）。
- mock 微基准：`engine/tsm_merge_cursor_bench_test.go`（`BenchmarkTotal_NoOverlap` 已得 N=10/100/1000
  的 CPU/分配数据，本端到端方案补充真实 I/O + 内存 + 多维度）。
- 设计文档：`tsmMergeCursor_unordered_merge_design.md` §8（性能）、§10.1（覆盖矩阵）、§9（segment
  排序前提——端到端真实文件可顺带验证此前提）。
