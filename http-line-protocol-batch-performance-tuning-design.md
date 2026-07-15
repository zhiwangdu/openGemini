# HTTP 行协议大点拆批性能与 `ReadBlockSize` 调优设计

> 文档定位:独立性能专题，不属于 `uint32-offset-overflow-fix-design.md` 的安全修复范围，也不作为该修复的开发或发布准入条件。
>
> 目标:评估 10KiB 级大点写入时，HTTP 行协议按 `ReadBlockSize` 拆批造成的路由、RPC、marshal、store write 和 WAL 固定开销，并用压测数据决定是否调整配置或实施后续聚合优化。

---

## 一、范围与非目标

本设计只覆盖 TSStore 普通 HTTP `/write` 行协议路径的性能测试和配置调优。

- 不处理 `ColVal.Offset`、`ChunkMeta.size` 或其他 uint32 溢出问题。
- 首轮只评估 `ReadBlockSize` 配置，不改变 parser、shard、owner、retry 或 WAL 的现有写入语义。
- 没有完整压测数据时保持 64KiB 默认值。
- parser block 与 per-shard storage batch 解耦、跨 block 聚合和重复 marshal 消除作为独立后续优化，不与配置调优绑定交付。

---

## 二、当前链路与拆批语义

普通行协议写入并不是“一个 HTTP request 对应一个 store write 或 WAL record”。实际链路为:

```text
HTTP request
  -> N 个按完整行对齐的 ReadBlockSize parser batch
  -> 全局 worker 并发执行各 batch 的解析和完整写入，不跨 batch 合并
  -> 每个 parser batch 按 shard 拆分
  -> 每个 (parser batch, shard, owner / retry attempt) 生成 binaryRows
  -> Store.WriteRows
  -> 正常 TSStore、WAL enabled、非 Shelf 路径的一次成功 attempt
  -> 一个 WAL physical record
```

### 1. Parser block

`ReadBlockSize` 是目标 buffer 大小，不是严格的 parser batch 字节上限，也不会把一个点从中间切开。`ReadLinesBlockExt` 会退到最后一个换行，把未完成行复制到 `tailBuf` 并带到下一批；当前 buffer 内没有换行时会倍增扩容，直到读到完整行或触发 `MaxLineSize`。

复用 buffer 的 capacity 已经大于 `ReadBlockSize` 时不会主动缩回，因此批次点数只能作为常规配置下的近似值，压测必须同时记录实际 batch 行数和字节数。

### 2. Worker 与背压

`serveWrite` 每读出一个完整行块就创建独立 `UnmarshalWork`。全局 worker 数为 `P = cpu.GetCpuNum()`，队列容量为 `2P`；队列满后 `ScheduleUnmarshalWork` 阻塞并形成背压。

worker callback 同步执行完整 `RetryWritePointRows`，直到 shard、RPC、store 和 WAL 返回后才释放。因此 worker 并发覆盖端到端 parser batch，不只是并行解析；不同 batch 之间没有 request 级或 shard 级 coalesce，完成顺序也不保证与 HTTP body 中的行顺序一致。

### 3. Shard、owner 与 WAL 放大

每个 parser batch 内按 shard 聚合并并行写不同 shard，同一 shard 的 owners 顺序写入。实际调用次数可按下式理解:

```text
parser batch 数 ~= ceil(pointCount / max(1, floor(effectiveBlockCapacity / averageLineBytes)))
store write 数  = sum(distinctShards(each parser batch) * owners * attempts)
WAL record 数   = 各 store 节点上成功执行到标准 TSStore s.wal.Write 的次数
```

`effectiveBlockCapacity` 是本次读取实际复用或扩容后的 buffer capacity；在没有历史大 buffer 和超 `ReadBlockSize` 单行的常规场景中，可近似取配置的 `ReadBlockSize`。

HTTP request 和 parser batch 都不是跨 shard/owner 的原子写入单元。不同 batch 并发还可能使同一 series 的时间戳按不同顺序进入 memtable；`WriteChunk.Mu` 会串行实际 append，但乱序到达可能使后续 flush 增加排序成本。

---

## 三、10KiB 大点量级估算

当前代码默认 `ReadBlockSize` 为 64KiB、`MaxLineSize` 为 1MiB；非 gzip、正常受 `max-body-size` 限制的 HTTP wire body 默认上限为 25MB。

按平均每点 10KiB、每个 request 1000 点、单 shard/单 owner、复用 buffer capacity 等于配置值估算:

| `ReadBlockSize` | 每个 parser batch 约含点数 | parser batch / store write / WAL record 约数 |
|---|---:|---:|
| 64KiB | 6 | 167 |
| 256KiB | 25 | 40 |
| 512KiB | 51 | 20 |
| 1MiB | 102 | 10 |

64KiB 下约产生 167 次路由、marshal、store write、Snappy encode、WAL header 和文件 `write`。如果每个小 batch 命中多个 shard，调用次数会按每批 distinct shard 数进一步增加，理论上可接近每点一次 store write。

该放大主要影响固定开销、锁竞争、RPC 和文件 syscall，不表示整体吞吐会随 batch 数等比例下降；解析、字符串复制、marshal、memtable append 和 Snappy 扫描等 `O(总字节数)` 工作仍然存在。

不同部署还存在以下差异:

- 分离部署的 `ts-sql -> ts-store` 路径对每个 `(batch, shard, owner)` 执行一次 `FastMarshalMultiRows`、同步 RPC 和 store 端 `FastUnmarshalMultiRows`，小 batch 会放大 RPC 固定成本。
- 组合部署 `ts-server` 的普通非 stream `LocalStore` 路径当前先通过 `netstorage.MarshalRows` 调用一次 `FastMarshalMultiRows`，随后又覆盖同一 buffer 调用一次 `FastMarshalMultiRows`。它通常不是两份 binary buffer 同时常驻，但会对大 String field 做两次完整遍历和复制。
- 同一 series 的 append 最终受 `WriteChunk.Mu` 串行，parser batch 并发不能消除该串行段，反而可能增加竞争和乱序 flush 排序。
- 每个 WAL record 都会独立 Snappy 编码并调用一次底层文件 `Write`。默认 `wal-sync-interval=100ms` 时，同一 WAL partition 在时间窗口内的多个 record 由后台 sync 合并，并非每 record 一次 `fsync`；interval 为 `0` 时才逐 record 同步。

---

## 四、性能与内存取舍

调大 `ReadBlockSize` 可以降低 parser batch、路由、RPC、store write 和 WAL record 数，但会提高并发内存:

- raw block 并发内存近似按 `O(P * ReadBlockSize)` 增长。
- 考虑运行中和队列内 raw block，以及 active `binaryRows` / Snappy buffer，活跃流水线可粗略按 `5P * ReadBlockSize` 评估。
- pool 高水位、历史扩容 buffer 和阻塞中的 HTTP handler 会进一步增加 retained bytes 与 RSS。
- 大点均匀分散到多个 shard 时，即使 parser block 变大，单个 per-shard batch 仍可能很小，收益会弱于单 shard 场景。

因此不能只比较 points/s；配置决策必须同时比较吞吐、尾延迟、CPU/GB、alloc bytes/GB、heap/RSS 高水位和 GC pause。

---

## 五、压测方案

### 1. 固定数据集

- 每点约 10KiB，1000 点/request。
- 预创建 measurement、series 和 shard，排除建表及首次路由抖动。
- 固定字段数量、String field 占比、时间戳分布和压缩率。
- 每组包含预热期、稳定采样期和多次重复，记录置信区间或至少给出波动范围。

### 2. 配置矩阵

- `ReadBlockSize`:64KiB、256KiB、512KiB、1MiB。
- 拓扑:单 shard/单 series、单 shard/多 series、均匀多 shard。
- 部署:组合部署 `LocalStore`、分离部署 RPC。
- WAL:enabled + 默认 100ms sync、enabled + 0ms sync、disabled。
- 并发:1、`P/2`、`P`、`2P`。

### 3. 必采指标

- MB/s、points/s、p50、p95、p99。
- CPU/GB、alloc bytes/GB、GC pause、heap/RSS 高水位。
- `delta(WriteRowsCount) / delta(WriteRowsBatch)`，用于判断实际 per-shard batch 点数。
- parser batch、store write、RPC 和 WAL physical record 数/request。
- WAL 文件 `write` 与 `fsync` 分别计数，不能用 record 数替代 `fsync` 数。
- `performance.WriteRowsCount`、`WriteRowsBatch`、`WriteWalDurationNs`、`WriteRowsDurationNs`、`WriteUnmarshalNs`、`WriteStorageDurationNs`、`WriteGetTokenDurationNs`。
- `httpd.writeReqParseDurationNs`、`scheduleUnmarshalDns`、`writeStoresDurationNs`、`writeReqDurationNs`。
- parser/unmarshal work pool retained bytes、store write limiter 等待时间。

duration 类指标是并发累计值，需要按 bytes、points 或 batch 归一化，不能直接当作墙钟时间比较。`scheduleUnmarshalDns` 增长用于识别全局 worker queue 背压。

### 4. Profile 要求

- 组合部署采集 CPU/alloc profile，确认重复 `FastMarshalMultiRows` 的占比及 buffer 生命周期。
- 分离部署区分 SQL 端 marshal、网络、store 端 unmarshal 和 write limiter 等待。
- 对比各 block size 时检查 pool 高水位后的 buffer 保留，避免吞吐收益依赖不可接受的常驻内存增长。

---

## 六、配置决策、放量与回滚

### 决策原则

- 没有覆盖全部拓扑和部署形态的数据时保持 64KiB 默认值。
- 只有在吞吐或 CPU/GB 有稳定收益、p99 不退化且 heap/RSS 高水位满足预算时，才选择更大的候选值。
- 不能用单 shard 结果代表多 shard；必须同时检查 `delta(WriteRowsCount) / delta(WriteRowsBatch)` 和 RPC/WAL record 数。
- 256KiB、512KiB、1MiB 逐级比较，不直接从 64KiB 跳到最大值。

### 放量

1. 先在压测环境完成全部矩阵。
2. 再按节点或部署组逐级放量候选配置，每一级保持足够观察窗口。
3. 每一级比较吞吐、p99、CPU/GB、heap/RSS、GC、实际 per-shard batch 点数和 write limiter 等待。
4. 不与 uint32 安全修复开关捆绑发布或回滚。

### 回滚

出现以下任一情况时回退到上一级或 64KiB:

- heap/RSS 或 retained buffer 超出预算。
- GC pause、p95/p99 或 worker queue 等待明显恶化。
- 多 shard 场景实际 per-shard batch 没有提升，吞吐收益不足以抵消内存成本。
- RPC、store write 或 WAL record 数未按预期下降。

---

## 七、后续优化候选

### 1. 消除组合部署重复 marshal

普通 `LocalStore` 路径的重复 `FastMarshalMultiRows` 是相对独立、语义风险较低的候选优化。修改后必须验证 stream 分支、`IndexKey` buffer 生命周期、错误传播和内存复用行为不变。

### 2. Parser block 与 storage batch 解耦

如果仅调大 `ReadBlockSize` 仍无法改善多 shard 小 batch，后续可评估跨 parser block 的 per-shard 聚合。但实施前必须定义:

- 聚合字节上限和最大等待时间。
- worker queue 与下游 store 的背压。
- 同 series 顺序和跨 batch 完成顺序。
- 跨 shard/owner 的部分成功语义。
- retry attempt 去重和故障恢复行为。
- 内存池高水位与关闭/取消时的 buffer 释放。

该优化会改变写入调度和错误边界，不作为 `ReadBlockSize` 配置调优的默认实现。
