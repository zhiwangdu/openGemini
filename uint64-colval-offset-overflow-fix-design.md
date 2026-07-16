# TSStore uint32 Offset 溢出修复：方案总览与拆分

> 文档状态：总览文档，代码尚未实施。
>
> 调研日期：2026-07-16。
>
> 本文不再承载具体实现设计；细节以两份子方案为准。

## 一、文档拆分

原方案同时讨论了两个性质不同的问题：

1. `ColVal.Offset` 作为进程内数据结构，如何从 `uint32` 原子升级为 `uint64`，同时保持所有既有 wire/ABI 不变。
2. 单个 TSSP chunk 的真实大小超过 `MaxUint32` 后，writer、ChunkMeta、reader、compact/merge 和 snapshot 如何继续正确工作。

二者有前后依赖，但不是同一个变更：第一项是兼容性基础改造，第二项是 large-chunk 能力建设。为避免把“类型宽化可独立合入”误解为“已经允许生成大 chunk”，设计拆为：

- [`colval-uint64-foundation-design.md`](./colval-uint64-foundation-design.md)：ColVal uint64 兼容基础改造。
- [`tssp-large-chunk-enablement-design.md`](./tssp-large-chunk-enablement-design.md)：TSSP large-chunk 读写能力。

本文仅定义两份方案的边界、依赖和发布关系。

## 二、总决策

| 维度 | ColVal uint64 基础改造 | TSSP large-chunk 能力 |
|---|---|---|
| 目标 | 消除进程内 String/Tag offset 的 uint32 回绕 | 正确读写真实大小大于 `MaxUint32` 的 TSSP chunk |
| 核心变化 | `ColVal.Offset []uint64`，全生命周期和边界适配 | 内存 `ChunkMeta.size uint64`、真实范围恢复、large writer、reader/consumer、流式 snapshot |
| 既有格式 | TSSP、Record、Shelf/WAL、executor 等格式均不变 | Segment 和 ChunkMeta 的物理字段宽度仍不变 |
| 单 segment 上限 | 仍为 `MaxUint32`，编码前 checked narrow | 仍为 `MaxUint32`；通过多个合法 segment 组成大 chunk |
| 单 chunk `> MaxUint32`（即 `>= 4GiB`） | 不允许，也不承诺可用 | TSStore attached 路径支持 |
| 可独立评审/合入 | 可以 | 可以，但依赖基础改造契约 |
| 可独立生产发布 | 仅在代码强制 `LegacyChunk32` 边界时 | 采用 reader-first 的 R1/R2 发布 |

总体方向不再使用 shard 级 `ErrValueTooLarge` 作为单 series/单列 4GiB 的常规保护。该保护会让一个热 series 连带拒绝同一 shard batch 中的其他 series，尤其不适合当前“大 String、多 series、短时 GB 级写入”的车联网负载。

资源不足仍需按 node/shard 的可观测内存预算背压；这是资源保护，不是 uint32 格式保护。

## 三、依赖关系

```text
ColVal uint64 基础改造
        │
        │ 交付稳定的内存模型和 legacy 边界契约
        ▼
TSSP large-chunk Reader/Consumer（R1，writer 关闭）
        │
        │ 所有目标节点完成 reader-first 升级
        ▼
TSSP large-chunk Writer/Snapshot（R2，writer 开启）
```

依赖是单向的：large-chunk 方案依赖 ColVal 基础改造，ColVal 基础改造的单元测试和正确性不应依赖 large-chunk writer。

两份方案的生产能力均以 64 位 Go 架构为前提；所有 uint64 offset 转 Go slice index 前仍需校验 `MaxInt` 和实际 buffer 长度。32 位进程不能承担相关 write/large-reader role。

### 3.1 基础改造向 large-chunk 方案交付的契约

基础改造完成后必须保证：

- String/Tag 的内存 offset 全程为 uint64，append、slice、sort、merge、update 和 delete 均不会在中途窄化。
- TSSP String segment 编码仍使用原有 uint32 wire；编码时 checked narrow，解码时扩宽为 uint64。
- segment-local offset 追加到目标 `ColVal` 时，按 `uint64(len(dst.Val))` 正确 rebase。
- `Segment.size` 仍为 uint32，并在完整 segment 编码完成后做 checked cast。
- Record、Shelf/WAL、Arrow、executor、index/CGO 等旧边界保持原有格式或 ABI；每个窄化点都显式检查。
- 普通范围数据的编码字节与改造前一致，并有 golden test 固化。
- 不以“新集群没有历史文件”替代边界检查。新写入、WAL replay 和内存转换同样可能到达边界。

### 3.2 large-chunk 方案不得反向改变的契约

large-chunk 方案不得：

- 把任何既有 32 位 wire 字段直接升级为 64 位。
- 依赖 `unsafe` 把 `[]uint64` 按 `[]uint32` 使用。
- 放宽单个 String value 或单个 encoded segment 的 uint32 硬边界。
- 为实现大 chunk 改变普通 chunk 的磁盘字节语义。
- 把 `ChunkMeta.size` 的低 32 位单独当成真实 I/O 长度。

## 四、职责边界

### 4.1 仅属于 ColVal uint64 基础改造

- `record.ColVal.Offset` 类型和完整生命周期。
- 进程内内存计量与 legacy codec size 的语义拆分。
- TSSP String/Tag segment 的 pack/unpack 宽窄转换和 decoder 校验。
- immutable decode scratch、segment decode 后的 uint64 rebase。
- Record codec、Shelf/WAL、consume、Arrow、executor、skip/fulltext index 和 CGO 边界适配。
- 编译器不能发现的显式 cast、unsafe、Tag 分支和语义累加审计。
- 普通 `<= MaxUint32` 数据的格式兼容与回归测试。

基础改造只声明“内存 offset 不再回绕”，不声明“单个 encoded chunk 可以达到 4GiB（即超过 `MaxUint32`）”。

### 4.2 仅属于 TSSP large-chunk 能力

- `ChunkMeta.size` 在内存中直接升级为 `uint64`，并始终表示真实 chunk data 长度。
- fixed/self-compressed codec 继续只写低 32 位；磁盘 size 逐步降级为兼容和一致性校验字段。
- engine/layout policy 在打开文件时固化到 file reader；TS attached 与 Legacy32 decoder 不允许自行猜测。
- TS attached full/selected decoder 扫描全部 wire ColumnMeta/Segment 元数据后恢复真实 size；selected query 只减少物化，不减少范围扫描。
- query preload 的范围检查和 direct-read 一次性 fallback。
- whole-chunk consumer 的流式能力矩阵。
- `WriteOriginal`、compact、merge、BufferReader 等大范围处理。
- writer 的列/chunk 累计宽化，以及基于 `writer.DataSize()` 的真实物理游标。
- 大 chunk 的 segment-streaming snapshot 和有界 encoded 工作集。
- 历史 absolute-offset 污染的文件级顺序审计、告警和隔离。
- TSStore attached writer 的 reader-first 发布；ColumnStore/detached 的 `LegacyChunk32` 策略。

### 4.3 共享代码的归属规则

两份方案可能修改同一 package，但同一语义只能由一份文档定义：

- String block 的 offset 宽窄转换归基础改造；large-chunk 仅使用其结果。
- segment 最终长度的 checked cast 归基础改造；large-chunk 负责规划多个 segment 和 chunk 级范围。
- `ChunkMeta` wire 不变是共同约束；其真实范围恢复和 consumer 使用方式归 large-chunk。
- reader 中 `ColVal.Offset` 类型适配归基础改造；reader 的真实 chunk 范围、预读和 fallback 归 large-chunk。

实现顺序建议先合入基础改造，减少后续 large-chunk PR 同时承担全仓类型迁移造成的评审噪声。

## 五、兼容性结论

### 5.1 正常范围数据

对所有既有 32 位范围内的数据：

- 新版本能读取旧版本生成的文件、WAL 和 Record payload。
- 基础改造后的新版本继续生成原格式字节，旧版本仍可读取。
- 单纯将内存 offset 扩宽，不要求 TSSP version、String block version、WAL version 或 Record wire version 升级。

### 5.2 大 chunk

当真实 chunk 大小超过 `MaxUint32` 时：

- 磁盘中的既有 `ChunkMeta.size` 字段只能保存低 32 位；新版本内存中的 `ChunkMeta.size uint64` 由完整 metadata 恢复真实长度。
- 只有完成 large-chunk reader/consumer 能力的版本可以安全处理。
- 旧版本不在兼容范围内；因此 writer 必须在 reader-first 升级后才能启用。
- 已由历史 writer 写坏的绝对 `Segment.offset` 或后续 SID cursor 不在自动恢复范围，由独立审计工具识别和隔离。

### 5.3 新建空集群

“新创建、没有数据的集群”能消除历史文件迁移问题，但不能消除新数据到达 4GiB 边界的风险。因此它不能替代：

- legacy wire 的 checked narrow；
- 单 segment 硬边界；
- large-chunk writer 启用前的 reader capability；
- 未支持路径上的 `LegacyChunk32` admission。

## 六、PR 与发布策略

### 6.1 代码合入

建议拆为至少两个独立 PR 系列：

1. **Foundation PR**：完成 ColVal uint64 全仓原子迁移，保持外部格式和普通行为不变。
2. **Large-chunk PR**：在 foundation 契约上实现 ChunkMeta、reader/consumer、writer 和 snapshot 能力。

Foundation PR 可以独立编译、测试、评审和合入。其完成标准不是“编译器报错全部修完”，而是所有 wire/ABI、显式 cast、unsafe、内存计量及语义 rebase 均完成审计。

### 6.2 生产发布

Foundation 单独生产发布只有两个安全选择：

1. 所有尚未具备 large-chunk 能力的 writer 路径，在写文件字节前以代码强制 `LegacyChunk32` 边界；或
2. 与 large-chunk reader/consumer 能力按同一发布计划交付。

对当前目标业务，第一种选择会在过渡期重新引入 chunk 级等待、轮转或不可拆值错误。因此推荐“独立开发与合入、统一安排生产能力闭环”，并按以下顺序上线：

- **R1**：部署 ColVal uint64 基础改造和 `ChunkMeta.size uint64` reader 能力，覆盖所有可能接触目标文件的 query、compact、merge、rewrite、export 和运维工具；large-writer gate 保持关闭，所有 finalizer 仍强制 `LegacyChunk32`。
- **R2**：确认 R1 覆盖、存量文件审计和 segment-streaming snapshot 完成后，仅对已经闭环的 TSStore attached 路径启用 large-writer gate；ColumnStore、detached 等继续保持 `LegacyChunk32`。

不允许在混部期间让新 writer 持续生成旧 reader 可能读取的大 chunk。

回滚不能只以“是否已经落出 large 文件”为判断。active/sealed、待 flush 数据和后台 rewrite 也可能产生 large chunk；必须先关闭新 admission、完成 drain，并按文件和任务 capability inventory 选择回滚目标。

## 七、独立工作项与能力门禁

以下工作不并入两份设计的实现范围，但会影响对应路径能否声明 large-chunk capability：

- WAL replay 的分批 checkpoint/中途 flush 优化。
- merge streamMode 的数值和工作集上限优化。
- ColumnStore/detached 的 large-chunk snapshot、PK/fragment/index consumer 设计。
- 历史 TSSP 文件的 offset 污染审计和隔离。

在独立工作完成前，相应路径必须 fail closed、转到已有安全实现，或维持 `LegacyChunk32`，不能仅因 `ColVal.Offset` 已经是 uint64 就声明支持大 chunk。

## 八、总体验收门槛

两份方案联合完成时，应满足：

- 普通数据的 TSSP、Record、WAL 和相关 codec byte-for-byte 兼容。
- String 和 Tag 的所有内存路径均为 uint64，无隐式或静默窄化。
- 每个 segment 保持既有 uint32 wire 并在边界失败前不修改目标状态。
- 内存 `ChunkMeta.size` 只有真实 uint64 长度一种语义；wire low32 只存在于 codec 边界。
- TSStore attached full、selected 和 sequence decode 都扫描完整物理 metadata；attached 文件不能误用 Legacy32 policy。
- TSStore attached 能生成、查询、compact/merge、rewrite 和恢复真实大小 `> MaxUint32`（即 `>= 4GiB`）的 chunk。
- writer 的列累计、chunk 累计和下一 SID 游标不存在 uint32 中间截断。
- query large chunk 按 Segment 读取；whole-chunk consumer 要么有有界路径，要么在输出前 fail closed。
- large snapshot 使用 segment-streaming 路径，不构造完整 multi-GiB encode buffer。
- 历史仅 size 回绕文件可恢复；absolute-offset 污染由文件级审计识别并隔离。
- reader-first capability 能阻止旧 reader 重新接管 large 文件。
- WAL replay 和 merge streamMode 的独立风险不被误声明为已解决。

具体测试用例、数据结构和代码改动清单分别见两份子方案，本文不重复维护。

## 九、关联文档

- [`uint32-offset-overflow-fix-design.md`](./uint32-offset-overflow-fix-design.md)：原始受控回绕/背压方案，保留用于对比，不作为新方案实现依据。
- [`uint32-offset-overflow-tssp-audit-tool-design.md`](./uint32-offset-overflow-tssp-audit-tool-design.md)：历史文件审计工具设计。
- [`uint32-offset-overflow-fix-step1-delivery-order.md`](./uint32-offset-overflow-fix-step1-delivery-order.md)：既有分步交付讨论，最终顺序以两份新子方案为准。
