# TSStore uint32 溢出修复第一步设计

> 关联文档:
> - `uint32-offset-overflow-panic-analysis-tsstore.md`
> - `uint32-offset-overflow-fix-codex-dialogue.md`
> - `uint32-offset-overflow-fix-phase2-design.md`（第二步:灰度、回滚与运行治理）
>
> 开发阶段:本文件只定义第一步的强制安全修复与验证，暂不实现运行时灰度开关、按租户/节点放量和开关式回滚能力；这些运行治理能力拆到第二步设计。
>
> 目标:在不改变 TSSP 线格式的前提下，消除 TSStore 中 String field 列跨 record、segment 累积后导致的 `ColVal.Offset` 回绕、数组越界 panic 和坏数据落盘；同时把 `ChunkMeta.size` 作为不可信元数据处理，避免读/compact/merge 依赖已回绕的 chunk size。

---

## 一、范围与前提

**主线：防止 TSStore 持久化 String field 的 `ColVal.Offset` 在跨 record、segment 累积时溢出。**

**配套：把回绕的 `ChunkMeta.size` 视为不可信元数据；不修改其他存储引擎和线格式。**

### 1. 修复范围

**仅覆盖 TSStore（ts）的写入、落盘、compact、merge 和读取链路。**

- 包含 memtable append、snapshot/flush、非流式 compact/merge、stream compact、stream merge 和查询读取。
- ColumnStore（cs）、colstore compact、detached primary key/data/index 等路径不在本方案范围内。

### 2. 保护对象

**主保护对象是持久化 String field 的 `ColVal.Offset`；tag 和查询结果中的 aux tag 不纳入主线。**

- `tsMemTableImpl.WriteRows` 只把 `row.Fields` 传给 `appendFields` / `AppendFieldsToRecord`；tag 通过 series key / `tagSets` 参与 series 定位和过滤，不作为数据列 append 到持久化 `Record.ColVals`。
- 查询侧按 series 迭代数据，tag filter 从 `PointTags` 取值，不依赖 TSSP 数据列。`TagAuxHandler` 虽可把 aux tag append 到查询结果 `Record`，但它不属于 TSStore 落盘、snapshot、compact 或 merge 路径。
- 单个 String field 值仍受产品写入上限约束；单值超限时必须在 memtable mutation 前拒写。

### 3. 单 segment 边界

**当前配置已经保证单个合法 segment 及 stream compact 临时对象低于 uint32 边界，真正需要处理的是跨 record、segment 的无界累积。**

- `util.DefaultMaxRowsPerSegment4TsStore` 为 1000，当前 `max-rows-per-segment` 保持 1000。
- HTTP 行协议完整单行受 `max-line-size` 限制：代码默认值为 1MiB，仓库示例配置为 64KiB。String field 解码只去除引号和转义，单列值字节数不会大于所在输入行。
- `continueMerge` 仅在当前 `c.col.Len < maxRowsPerSegment` 时跨输入合并；追加下一个最多 1000 行的源 segment 后，`splitColumn` 立即按 1000 行切分。因此输出 segment 最多 1000 行，临时 `c.col` 严格少于 2000 行。
- 按 `max-line-size=1MiB` 保守估算，输出 segment 的单个 String field `ColVal.Val` 不超过约 1000MiB，stream compact 临时对象低于约 2000MiB，均小于 uint32 的 4GiB 表示边界。
- `decodeColumnData` / `DecodeStringBlock` / `unpackStringV1/V2` 接收的是 segment 级编码块。在上述边界成立时，它们不是本次跨 segment 累积溢出的主修点。

该结论以 `max-rows-per-segment=1000`、`max-line-size<=1MiB` 且数据经过行协议限制为前提。未来提高任一上限或引入绕过该限制的写入入口时，必须重新评估；本次不增加配置组合校验。

### 4. Stream 路径处理原则

**stream compact 保持现状；stream merge 本次只修复 `WriteOriginal` 截断复制，unordered 时间范围读取风险留给单独的乱序合并优化。**

- stream compact 不增加 rows + bytes 条件，也不在 bytes 达阈值时提前 `writeSegment`。
- stream merge 读取 unordered 时只有 `maxTime` 时间边界，可能一次聚合多个 segment，极端情况下形成超过 4GiB 的 String `ColVal`；这是已知风险。
- 本次不修改 `ReadTimes` / `Read`、`readUnordered`、`MergeHelper` 或 `columnWriter`，也不增加 `rowsLimit`；该问题在单独的乱序合并优化中处理。
- 本次 stream merge 的核心修复是 `WriteOriginal` 不再按可能回绕的 `meta.size` 复制，避免只复制 chunk 前缀而丢失数据。

### 5. 格式与类型选择

**保持现有 uint32 线格式，不把 `ColVal.Offset` 改成 uint64。**

- `ColVal.Offset`、TSSP string block offset/length、`Segment.size` 和 `ChunkMeta.size` 继续使用现有 uint32 表达。
- `ColVal` 是共享核心结构，改为 uint64 会扩大到大量内存结构、编解码和读写边界，超出本次修复范围。
- 本方案通过 mutation 前预算、bounded append、现有 segment 边界和流程降级控制本次范围内的风险；源 `ChunkMeta.size` 在读、compact、merge 侧按不可信元数据处理。stream merge 的 unordered 时间范围读取风险作为明确例外，留给单独优化。

---

## 二、核心策略

### 0. 两个数据大小维度

本问题可以拆成两个不同粒度的数据大小维度:

| 维度 | 对应字段 | 含义 | 本方案定位 |
|------|----------|------|------------|
| 单列变长数据大小 | `ColVal.Offset` | String field 单列的 `ColVal.Val` 字节大小；`Offset` 记录每个值在该列 `Val` 中的位置 | 主保护对象，通过 append 前预算、bounded append 或既有 segment 边界避免回绕；stream merge unordered 读取风险单独处理 |
| 单 series、单文件 chunk 数据大小 | `ChunkMeta.size` | 一个 `ChunkMeta` 描述的 chunk data 区大小，包含该 series 在当前 TSSP 文件内的 time 列和各 field 列 segment 数据 | 输出侧维持现状；读、compact、merge 侧把它视为不可信元数据 |

在当前 TSStore TSSP 组织下，同一 series 在一个 TSSP 文件内只对应一个 `ChunkMeta`，不会拆分成多个 `ChunkMeta`。该 `ChunkMeta` 描述该 series 在当前文件内的完整 chunk data 区；因此这里的大小维度是“单 series、单文件”，不是全文件或全 measurement。

这两个维度不能相互替代:`ChunkMeta.size` 描述的是落盘后的 chunk data 字节范围，通常是压缩/编码后的数据；`ColVal.Offset` 描述的是 decode 后单列 `ColVal.Val` 内部的偏移。即使 `ChunkMeta.size` 看起来可信，decode 后的 String field `ColVal.Offset` 仍可能越界，必须单独校验。

### 1. 主保护对象:`ColVal.Offset`

真正必须阻止的是 String field `ColVal.Val` 在内存中跨 record / segment 无界累积到 uint32 上限以上。本次覆盖的路径必须在 mutation 前做 bytes 预算、使用 bounded append，或已有可靠的 segment 行数边界。stream compact 由现有 segment 行数约束；stream merge 的 unordered 读取目前只有时间边界，尚不满足这一要求，本次只登记风险，不修改其读取流程。

核心不变量:

> 除按 rows + `max-line-size` 已有界的 stream compact 外，本次改造覆盖的跨 record / segment 累积路径在追加 TSStore String field 到目标 `ColVal` 前，必须保证追加后 `len(dst.Val) <= MaxVarColValBytes`。stream merge unordered 时间范围读取是已知未覆盖项，由单独的乱序合并优化补齐。

建议阈值:

- `MaxVarColValBytes`:默认 256MiB，可配置到 512MiB。
- 测试阈值:支持 1KiB / 64KiB 等小阈值，用于验证 flush、split、降级和 fail-closed 行为。

### 2. `ChunkMeta.size` 策略:输出将错就错，读侧不可信

`ChunkMeta.size` 是 uint32，表示一个 series 在当前 TSSP 文件内唯一 chunk 的 data 区总长度。当前依赖点不多，本方案不把它作为输出侧强保护目标:

- compact / merge 等后台流程写新文件时，`ChunkMeta.size` 维持现状，仍可能按 uint32 回绕写入。
- 不引入 `MaxChunkDataBytes`，也不按 bytes 将同一 series 在同一文件内拆成多个 `ChunkMeta`。
- 但所有后续流程不得无条件相信源 `ChunkMeta.size` 是真实 chunk data 长度。

判断源 `ChunkMeta.size` 是否可信时，先计算不依赖 `cm.size` 的 `expectedChunkDataSize`。实际落地可按场景选择算法:

1. 后继 `ChunkMeta` offset 差值:
   - 若当前 `ChunkMeta` 不是文件物理顺序中的最后一个 `ChunkMeta`，使用 `nextChunkMeta.offset - currentChunkMeta.offset`。
   - `nextChunkMeta` 必然属于同一文件中的另一个 series，不是当前 series 的第二个 chunk。该差值表示当前 series 的 chunk data 起点到下一 series 的 chunk data 起点之间的真实跨度。
2. segment entry 覆盖范围:
   - 使用 `max(entry.offset + entry.size) - cm.offset`。
   - 该方式不依赖后继 `ChunkMeta`，适合当前 `ChunkMeta` 位于文件物理顺序末尾，或调用方只能取得当前 `ChunkMeta` 的场景。

若 `expectedChunkDataSize > int64(cm.size)`，说明源 `ChunkMeta.size` 已截断、回绕或元数据损坏。此时:

- 非流式 compact / merge 不能继续用 `cm.size` 整 chunk 读取。
- 查询路径不能继续依赖 `cm.size` 做整 chunk 预读。
- `WriteOriginal` 不能用 `meta.size` 作为 fast-copy 的复制长度。

### 3. 重要约束:writer 内部 offset 不能依赖回绕 size

虽然输出的 `ChunkMeta.size` 可以将错就错，但 writer 内部推进 offset 不能依赖已回绕的 `chunkMeta.size`。否则会把文件中后续 series 的 `ChunkMeta.offset` 写错，问题就不再只是 size metadata 不准，而是文件布局损坏。

因此:

- stream compact / stream merge 当前主要用 `writer.DataSize()` 定位新 chunk，方向是合理的。
- snapshot / flush builder 中如使用 `chunkMeta.size` 推进 `dataOffset`，需要改为使用实际编码/写入字节数。

---

## 三、模块风险总览

| 模块 | 主要流程 | 风险点 | 处理原则 |
|------|----------|--------|----------|
| 写入 / memtable | 行协议解析 -> record append -> memtable mutation | String field `ColVal` 持续累积，append 前无 bytes 预算会形成回绕 offset | mutation 前预算，超限 flush/retry 或拒写 |
| WAL | shard `binaryRows` -> snappy physical record -> WAL header | 在现有行协议单行限制和写入批量约束下，单个 payload 不可能达到 uint32 表示边界 | 本次不修改 WAL，也不增加 payload 长度比较 |
| snapshot / flush | memtable record -> `MsBuilder` -> TSSP chunk | 写入侧预算必须保证输入 `Record` 的 String offset 有效；内部 `dataOffset` 不能依赖回绕的 `chunkMeta.size` | 写出前校验 `Record`，保留现有 rowsLimit / segment 行数切分；offset 用真实写入大小推进 |
| 非流式 compact / merge fastmode | chunk 整块读取 -> decodeRecord -> record merge -> 写新文件 | 整 chunk 读取依赖源 `ChunkMeta.size`；decode 后 `ColVal.Offset` 仍可能越界；record merge 会继续累积 `ColVal` | 源 size 不可信或 decode 后 offset 越界时降级 streamMode；merge 使用 bounded append |
| stream compact | segment 读取 -> compactColumn -> writeSegment -> writeMeta | 源读按 segment 安全；当前 rows 上限与 `max-line-size` 组合已保证输出 segment 和临时合并对象低于 uint32 边界 | 保持 row-only 合并与现有 `writeSegment` 时机；输出 `ChunkMeta.size` 维持现状 |
| stream merge | ordered/unordered segment merge 或 `WriteOriginal` fast-copy | unordered 读取只有时间边界，可能聚合超过 4GiB；`WriteOriginal` 信任回绕的 `meta.size` 会截断复制并丢数据 | unordered 读取风险转入单独优化，本次不改流程；核心修复 `WriteOriginal` 的真实复制范围 |
| 查询 | chunk meta -> segment data -> decode | `defaultIoSize` 整 chunk 预读依赖 `cm.size`；历史坏 offset 不能 panic | 预读失败/校验不通过时降级按 segment 读；fail-closed |

---

## 四、共享能力

### 1. String field 预算 API

新增或收敛到一组 bounded append 能力:

- `isStringField(typ int) bool`
- `VarBytes`
- `VarBytesRange`
- `CanAppendVarBytes`
- `TryAppendColVal`
- `TryAppendString`
- `TryAppendStringNull`
- `ValidateCol`
- `ValidateRecord`

错误语义:

- `ErrNeedFlush`:追加本身合法，但目标对象接近阈值，需要调用方先 flush/split/writeSegment 后重试。
- `ErrValueTooLarge`:单值或不可拆单次追加超过产品上限，拒写。
- `ErrCorruptColumn`:源 offset 或源 meta 不可信，不能继续 append 或 fast-copy。

### 2. 源 chunk size 校验

新增 `ValidateChunkMetaDataRange(cm *ChunkMeta, next *ChunkMeta, fileDataSize int64) (expectedSize int64, err error)`。`next` 表示同一 TSSP 文件物理顺序中的下一个 `ChunkMeta`，必然对应另一个 series；`cm` 是文件内最后一个 `ChunkMeta` 时传 `nil`:

- 存在 `next` 时，优先使用 `next.offset - cm.offset` 计算 expected size。
- 没有 `next` 或需要校验 segment 覆盖范围时，遍历所有 column/time segment entry。
- segment entry 算法需要校验 `entry.offset >= cm.offset`。
- 校验 `entry.offset + int64(entry.size)` 不溢出且不超过文件 data 区。
- 计算 `expectedSize = max(entry.offset + entry.size) - cm.offset`。
- 若 `expectedSize > int64(cm.size)`，返回 size 不可信错误。

该校验不要求输出方修正 `ChunkMeta.size`，只用于读、compact、merge 入口判断是否可使用整 chunk 读取或 fast-copy。

---

## 五、模块设计

### 1. 写入与 memtable

#### 风险点

- memtable 中同一 series 的 String field `ColVal` 会跨多次写入持续增长。
- append 过程中不能出现部分列已 mutation、后续列因超限失败的半写状态。

#### 设计动作

1. 在 `appendFields` / `AppendFieldsToRecord` 前，按目标 measurement / series / field 汇总整次 shard batch 对各 String field 的新增 bytes，并校验单值大小；预算阶段只计数，不复制或追加 field data。
2. 使用 `current ColVal bytes + batch delta` 做整批预算；预算与后续 mutation 之间必须有同一锁、reservation 或等价同步，避免并发写入造成 TOCTOU。
3. 预算失败时尚未发生 data mutation。单值超过产品上限返回 `ErrValueTooLarge`；目标 active memtable 放不下时返回 `ErrNeedFlush`，先 rotate/snapshot/flush，再在新 active memtable 上重试。空 memtable 仍放不下才拒写。
4. 不为失败路径实现已追加字段的 mutation 回滚。`AddMemSize` / token 应在预算成功后申请或提交；如果现有时序必须提前申请，只释放资源 token，不回滚数据 mutation。

#### 验收点

- 低阈值下，同一 series String field 列接近阈值后继续写入，会在 mutation 前返回 `ErrNeedFlush`。
- 单值超限不产生 memtable mutation。
- 同一 batch 含多个 series / field 时，任一目标列预算失败都不会出现部分字段已经 append 的半写状态。

### 2. Snapshot / flush

#### 风险点

- snapshot 输入来自已完成 mutation 前预算的 memtable，正常情况下各 String field `ColVal` 已满足 bytes 边界；写出入口仍需拒绝 offset/length 不可信的异常 `Record`。
- `ChunkMeta.size` 输出可以将错就错，但 builder 内部不能使用回绕后的 `chunkMeta.size` 推进 `dataOffset`。

#### 设计动作

1. snapshot 写出前执行 `ValidateRecord`；发现 String field offset/length 不可信时返回 `ErrCorruptColumn`，不得尝试通过切分修复已经损坏的 offset。
2. 保留 `MsBuilder.WriteRecord` 现有的 rowsLimit `Record.Split`，以及 `EncodeChunk` 按 `maxRowsPerSegment` 生成 segment 的流程；不增加 `MaxVarColValBytes` 驱动的 record 切分。
3. `ChunkDataBuilder` 保持“一个 series 在一个 TSSP 文件内只有一个 `ChunkMeta`”的现有组织方式和 `ChunkMeta.size uint32` 输出行为，不引入将同一 series 拆成多个 `ChunkMeta` 的新表示。
4. `MsBuilder` 推进 `dataOffset` 时使用实际编码/写入字节数，例如 `len(encodeChunk)` 或 writer 实际 `DataSize` 差值，而不是 `chunkBuilder.chunkMeta.size`。

#### 验收点

- 低 `MaxVarColValBytes` 下，由 memtable mutation 前预算触发 rotate/flush；snapshot 不因 bytes 阈值改变现有 record 或 segment 切分边界。
- 构造 offset/length 不可信的 snapshot 输入，验证在进入 builder 编码前 fail-closed。
- 构造 chunk data size 回绕场景时，文件中后续 series 的 `ChunkMeta.offset` 仍按真实写入位置推进。
- 输出 `ChunkMeta.size` 可以保持 uint32 回绕，不作为本方案失败条件。

### 3. 非流式 compact / merge fastmode

#### 风险点

- 非流式路径会整 chunk 读取，再按 segment entry 切列数据。
- 如果源 `ChunkMeta.size` 已回绕，整 chunk 读取会拿到截断数据。
- `ChunkMeta.size` 只约束落盘 chunk data 的字节范围；整 chunk decode 后仍必须校验解压后的 String field `ColVal.Offset` 是否在 `ColVal.Val` 边界内。
- 非流式 record merge 还会把多个 segment / record 继续追加到目标 `ColVal`。

#### 设计动作

1. 进入非流式 compact / merge 前，对源 chunk 执行 `ValidateChunkMetaDataRange`。
2. 如果 `expectedChunkDataSize > int64(cm.size)`，不继续非流式整 chunk 读取，降级到 streamMode。
3. 如果整 chunk 读取成功，但 `decodeRecord` 后 `ValidateCol` / `ValidateRecord` 发现 String field offset 越界，同样不继续非流式 fast path，降级到 streamMode。
4. streamMode 逐 segment 读取，依赖 segment entry 的 `offset/size`，不依赖源 `ChunkMeta.size` 定位数据。
5. `decodeRecord` / `Record.Merge` 使用 bounded append，避免目标 String field `ColVal` 超阈值。
6. 对大 String field 场景，优先绕开整 record fast path 并使用现有 streamMode；stream compact 保持 row-only 边界。stream merge 的 unordered 时间范围读取不在本次增加行数限制，由单独的乱序合并优化处理。

#### 验收点

- 源 `ChunkMeta.size` 小于 expected size（由下一 series 的 `ChunkMeta.offset` 或 segment entry 计算）时，非流式 compact / merge 降级 streamMode。
- 整 chunk 读取后 decode 出的 String field `ColVal.Offset` 越界时，非流式 compact / merge 降级 streamMode。
- 降级后不发生 `columnData` slice panic。
- 非流式 record merge 超过低阈值时，输出多个 bounded record。

### 4. Stream compact

#### 风险点

- 源读取按 segment offset/size 进行，不依赖源 `ChunkMeta.size` 定位数据。
- `continueMerge` 只在当前 `c.col.Len < maxRowsPerSegment` 时跨输入继续合并；追加下一个合法源 segment 后，`splitColumn` 立即按 `maxRowsPerSegment` 切分。当前 1000 行配置下，输出 segment 最多 1000 行，临时 `c.col` 严格少于 2000 行。
- HTTP 行协议 `max-line-size` 默认 1MiB、仓库示例 64KiB。按默认值保守估算，单个 String field 的输出 `ColVal.Val` 不超过约 1000MiB，临时合并对象低于约 2000MiB，不会到达 uint32 的 4GiB 边界。
- 输出 `ChunkMeta.size` 当前在 `writeMetaToDisk` 中用 `uint32(writer.DataSize() - cm.offset)` 写入，本方案不强制改为防回绕。

#### 设计动作

1. 保持 `continueMerge` 的 row-only 条件和 `splitColumn` 按 1000 行切分的现有行为。
2. 不在 `lastSeg`、`tmpCol` append 到 `c.col` 前新增 `MaxVarColValBytes` 预算，不新增 bytes 达阈值时提前 `writeSegment` 的分支。
3. `writeMetaToDisk` 的输出 `ChunkMeta.size` 维持现状；后续读取方不得依赖它作为真实 chunk data 长度。
4. 将 `max-rows-per-segment=1000`、`max-line-size<=1MiB` 作为本设计前提；未来调整配置上限时另行评估，不在本次实现中增加配置校验。

#### 验收点

- stream compact 不因低 `MaxVarColValBytes` 提前 `writeSegment`，仍按现有 rows 条件合并和切分。
- 在当前配置前提下，输出 segment 不超过 1000 行，跨输入合并时临时 `c.col` 严格少于 2000 行。
- 即使输出 `ChunkMeta.size` 回绕，后续 stream 读取仍可按 segment entry 读取。

### 5. Stream merge / out-of-order merge

本节只区分两类问题：unordered 读取的内存越界风险，以及 `WriteOriginal` 的截断复制风险。前者本次不改，后者是本节的核心修复点。

#### 问题一：unordered 读取只有时间边界，本次不修改

- ordered 输入虽然按 segment 处理，但 `ReadTimes(maxTime)` / `Read(maxTime)` 只限制时间范围，不限制行数或 String bytes。
- 普通流程可能在一个 ordered segment 的时间范围内命中多个 unordered segment；`lastSeries && lastSeg` 使用 `math.MaxInt64` 时还可能读取该 series 剩余的全部 unordered 数据。
- 极端密度下，单次构造的 unordered `ColVal` 可能超过 4GiB 并导致 uint32 offset 回绕。
- 本次仅记录该风险，不修改 `ReadTimes` / `Read`、`readUnordered`、`MergeHelper`、`columnWriter` 或现有时序语义；该问题在单独的乱序合并优化中统一处理。

#### 问题二：`WriteOriginal` 截断复制并丢数据，核心修复

`WriteOriginal` 当前以 uint32 `meta.size` 作为复制总长度，并使用 `for readSize < meta.size` 循环读取。真实 chunk data 超过 4GiB 时，`meta.size` 回绕为较小值，fast-copy 只复制 chunk 前缀；随后写出的新文件缺少尾部数据，属于确定的数据丢失。

修复动作:

1. 在改写 `meta.offset` 和 segment entry 前，不再使用源 `meta.size` 作为复制长度，先计算 `expectedChunkDataSize`:
   - 当前 `ChunkMeta` 后面存在另一个 series 的 `nextChunkMeta` 时，使用 `nextChunkMeta.offset - meta.offset`。
   - 当前 `ChunkMeta` 位于文件物理顺序末尾时，使用 segment entry 覆盖范围。
2. fast-copy 范围固定为 `[meta.offset, meta.offset + expectedChunkDataSize)`；总长度、`readSize` 和 offset 运算使用 int64，单次读取 buffer 仍保持小块。
3. 复制完成后按目标文件的新起点平移 segment offset，再写入 meta；输出 `ChunkMeta.size` 继续维持现有 uint32 行为。
4. segment entry 覆盖范围不可信时禁用 `WriteOriginal`，改走逐 segment rewrite；无法安全重写时返回 corrupt error，不能继续截断复制。

验收点:

- 源 `meta.size` 回绕时，`WriteOriginal` 仍复制完整 `expectedChunkDataSize`，新文件不丢失 chunk 尾部。
- 分别覆盖“下一 series 的 `ChunkMeta.offset`”和“文件末尾 segment entry 覆盖范围”两种长度计算方式。
- segment entry range 不可信时不走 fast-copy。
- unordered 读取和 merge / `columnWriter` 流程保持现状；本次不引入 rowsLimit 或新的分批语义。

### 6. 查询路径

#### 风险点

- `tsspFileReader.ReadData` 中 `cm.size < defaultIoSize` 时会整 chunk 预读。
- 如果 `cm.size` 已回绕成小值，整 chunk 预读可能拿到截断 chunkData。
- 截断 chunkData 后续 `columnData` / decode 会失败或 panic。

#### 设计动作

1. 查询路径默认仍保留 `defaultIoSize` 整 chunk 预读优化。
2. 当源 chunk range 校验发现 `expectedChunkDataSize > int64(cm.size)`，或整 chunk 预读后的 decode/range check 失败时，降级为按 segment 依次读取。
3. 按 segment 读取使用 `Segment.offset/size`，不依赖源 `ChunkMeta.size`。
4. `columnData` 前增加边界判断，避免截断 chunkData 触发 slice panic；失败后走 per-segment fallback 或返回明确 corrupt error。

#### 验收点

- 构造回绕 `ChunkMeta.size` 且 segment entry 正确的文件，查询会降级到 per-segment read 并尽量完成读取。
- segment entry 也不可信时，查询返回 corrupt error，不 panic。

### 7. 其他依赖点核查

| 位置 / 流程 | 是否依赖 `ChunkMeta.size` | 处理 |
|-------------|---------------------------|------|
| `tsspFileReader.ReadData` 的 `validate(cm.offset, cm.size)` | 是，但只做文件范围粗校验 | 不能作为 size 正确性的证明；后续仍需整 chunk 预读 fallback 或 segment range 校验 |
| `tsspFileReader.ReadData` 的 `cm.size < defaultIoSize` | 是，决定是否整 chunk 预读 | 预读失败、range check 失败或 decode 失败时，降级 per-segment read |
| `ChunkIterator.readRecord` | 是，非流式 compact 读整 chunk | 入口 size 校验失败，或整 chunk decode 后 `ColVal.Offset` 越界时降级 streamMode |
| `mergePerformer.WriteOriginal` | 当前依赖，风险最高 | 改为按下一 series 的 `ChunkMeta.offset` 或 segment entry 覆盖范围复制，不使用源 `meta.size` |
| stream compact 读取源数据 | 否，按 segment offset/size 读取 | 不需要因源 `ChunkMeta.size` 回绕改读取逻辑 |
| unordered reader / SegmentReader | 否，按 segment offset/size 读取 | 不需要因源 `ChunkMeta.size` 回绕改读取逻辑 |
| first/last/min/max 等预聚合读取 | 基本不依赖，按目标 segment 读取 | 保持按 segment 读取；遇到 decode 错误 fail-closed |
| `ChunkMeta` marshal/unmarshal / self codec | 只是读写 uint32 字段 | 线格式不变，不新增输出保护；读侧把该字段视为不可信 |
| `MetaIndex.size` | 不是 `ChunkMeta.size`，用于 meta block 读取 | 不纳入本问题，保持现状 |
| snapshot / flush `dataOffset` 推进 | 当前可能间接依赖 `chunkMeta.size` | 改为使用真实编码/写入大小，避免 size 回绕污染后续 offset |

---

## 六、第一步开发交付内容

第一步一次性交付下列安全修复，不再在本步骤内按运行时能力开关、路径开关或后台任务类型拆分。所有保护逻辑按设计直接生效；第一步不新增灰度配置、灰度状态机和开关式回滚分支。部署放量、运行时开关、观测与回滚治理在第二步单独开发。

### 必须随版本交付

- 新增 `ValidateChunkMetaDataRange`，支持用后继 `ChunkMeta` offset（对应下一 series）或 segment entry 覆盖范围判断源 `ChunkMeta.size` 是否可信。
- 查询整 chunk 预读失败或校验不通过时，降级到 per-segment read。
- 非流式 compact / merge fastmode 发现源 `ChunkMeta.size` 不可信，或整 chunk decode 后 `ColVal.Offset` 越界时，降级 streamMode。
- `WriteOriginal` 复制长度改为依赖后继 `ChunkMeta` offset（对应下一 series）或 segment entry 覆盖范围，而不是源 `meta.size`。
- `appendFields` 前按整次 shard batch 汇总 String field bytes 并校验单值；预算失败不产生部分 data mutation，也不做 mutation 回滚。
- 单值超限拒写；追加会使 memtable `ColVal` 超阈值时返回 `ErrNeedFlush`，触发 snapshot/flush 后重试。
- 引入 String field 判断、bounded append API 和 `ValidateCol` / `ValidateRecord`。
- 除已有 row-only 边界的 stream compact 外，本次覆盖的高风险 `AppendColVal`、`AppendString`、直接 `uint32(len(cv.Val))` 统一收敛到 bounded API；stream merge unordered 时间范围读取作为已知未覆盖项转入单独优化。
- snapshot/flush 写出前校验 `Record`，保留现有 rowsLimit / segment 行数切分，不增加 bytes 驱动的切分。
- snapshot/flush 内部 offset 推进改为真实写入大小，不依赖回绕 `chunkMeta.size`。
- stream merge 本次只修复 `WriteOriginal` 的真实复制范围；unordered 读取、merge 和 `columnWriter` 流程保持现状，不增加 rowsLimit。
- 非流式 compact / merge 大 String field 场景绕开整 record fast path 并使用现有 streamMode；stream compact 保持现有 row-only 切分，stream merge unordered 读取风险转入单独优化。
- compact / merge 输出 `ChunkMeta.size` 维持现状，不作为本方案保护目标。

---

## 七、测试策略

### 写入 / snapshot

- 低 `MaxVarColValBytes` 下，同一 series String field 列接近阈值后继续写入，验证 mutation 前返回 `ErrNeedFlush`。
- 单值超过产品上限，验证不产生 memtable mutation。
- 同一 batch 构造多个 series / String field，其中后处理字段预算失败，验证前面的字段也没有发生 mutation。
- snapshot 写出前验证 `Record`；offset/length 不可信时在进入 builder 编码前 fail-closed。
- 低 bytes 阈值不改变 snapshot 现有 rowsLimit / segment 行数切分边界。
- 构造 `ChunkMeta.size` 回绕场景，验证 snapshot writer 内部后续 series 的 `ChunkMeta.offset` 仍按真实写入大小推进。

### `ChunkMeta.size` 读取降级

- 构造 `ChunkMeta.size` 小于 expected size 的源 meta，分别覆盖后继 `ChunkMeta` offset 和 segment entry 两种算法；前者明确使用下一 series 的 `ChunkMeta`，验证非流式 compact / merge 降级 streamMode。
- 构造 `ChunkMeta.size` 可信但 decode 后 String field `ColVal.Offset` 越界的整 chunk 数据，验证非流式 compact / merge fastmode 降级 streamMode。
- 查询路径触发 `defaultIoSize` 整 chunk 预读但 decode/range check 失败时，验证降级 per-segment read。
- segment entry 本身不可信时，验证查询/compact 返回 corrupt error，不 panic。
- stream compact / stream merge 输出回绕 `ChunkMeta.size` 后，后续 streamMode 仍可按 segment entry 读取。

### Compact / merge

- 非流式 compact / merge 的 record 合并总 bytes 超低阈值时，验证不会构造超阈值目标 `ColVal`。
- stream compact 复用现有 rows 边界测试，不新增低 `MaxVarColValBytes` 下提前 `writeSegment` 的测试预期。
- 源 `meta.size` 回绕且当前 chunk 后面存在另一 series 时，`WriteOriginal` 按下一 `ChunkMeta.offset` 推导的 int64 长度完整复制，不生成截断新文件。
- 当前 chunk 位于文件末尾时，`WriteOriginal` 按 segment entry 覆盖范围完整复制。
- segment entry range 不可信时禁用 fast-copy，并验证逐 segment rewrite 或 corrupt error 行为。
- 现有 unordered 读取、merge 和 `columnWriter` 回归测试结果保持不变；本次不新增 rowsLimit 相关测试预期。

### 兼容性

- 旧 TSSP 新二进制可读。
- 新二进制写出的 TSSP 仍保持旧格式，旧二进制可读。
- 含坏 offset / 坏 chunk meta 的旧文件，新二进制读不 panic。

---

## 八、改动清单

### 共享 record 能力

- `lib/record/column.go`:String field 判断、checked accessor、bounded append 入口。
- `lib/record/record_check.go`:新增 `ValidateCol` / `ValidateRecord`。

### 写入 / memtable

- `engine/mutable/ts_table.go`:按整次 shard batch 汇总 String field bytes，在任何字段 append 前完成预算；预算与 mutation 使用同一同步域或 reservation。
- `engine/shard.go`:处理 `ErrNeedFlush` / `ErrValueTooLarge`；调整 mem size/token 时序，失败只释放资源，不回滚 data mutation。
- `lib/errno`:新增或统一 `ErrNeedFlush` / `ErrValueTooLarge` 的错误语义。

### Snapshot / flush

- `engine/immutable/msbuilder.go`:写 record 前执行 `ValidateRecord`，保留现有 rowsLimit 切分；`dataOffset` 用真实写入大小推进。
- `engine/immutable/chunkdata_builder_ts.go`:保留“一个 series、一个文件、一个 `ChunkMeta`”及 `ChunkMeta.size uint32` 的输出现状，不新增多 `ChunkMeta` 表示或 chunk byte cap。
- `engine/immutable/column_builder.go`:保留 segment 写前断言。

### Compact

- `engine/immutable/chunk_iterators.go`:整 chunk 读取前校验源 chunk data range；整 chunk decode 后校验 `ColVal.Offset`；不可信或 offset 越界时触发 streamMode 降级。
- `engine/immutable/stream_compact.go`:本次不修改 `compactColumn` / `continueMerge` / `writeSegment`；`writeMetaToDisk` 的 `ChunkMeta.size` 输出维持现状。
- `engine/immutable/merge_tool.go` / `merge_self.go`:大 String field、源 `ChunkMeta.size` 不可信，或整 chunk decode 后 `ColVal.Offset` 越界时绕开整 record fast path。

### Merge

- `engine/immutable/merge_performer.go`:`WriteOriginal` 复制总长度改为下一 series 的 `ChunkMeta.offset` 或 segment entry 覆盖范围，并使用 int64 推进；`Handle -> readUnordered -> merge -> columnWriter` 保持现状。
- `engine/immutable/stream_downsample.go`:`StreamWriteFile.WriteMeta` 的 `ChunkMeta.size` 输出维持现状。

### 查询

- `engine/immutable/tssp_file_meta.go` / `chunk_meta_codec.go`:chunk data range 校验辅助。
- `engine/immutable/tssp_file.go`:整 chunk 预读失败或 size 不可信时降级 per-segment read；`columnData` 前增加边界判断。

---

> 第二步的灰度开关、放量、回滚、运行监控与总体决策摘要见 [`uint32-offset-overflow-fix-phase2-design.md`](uint32-offset-overflow-fix-phase2-design.md)。
