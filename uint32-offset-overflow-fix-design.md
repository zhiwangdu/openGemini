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

只考虑 TSStore（ts）路径。ColumnStore（cs）、colstore compact、detached primary key/data/index 等路径不在本方案范围内。

明确前提:

1. 本次不修改 stream compact 的 segment 合并/切分策略。调研确认当前边界足以排除单 segment 超过 4GiB:
   - `util.DefaultMaxRowsPerSegment4TsStore` 为 1000，`max-rows-per-segment` 按当前运行配置保持 1000。`continueMerge` 只在当前 `c.col.Len < maxRowsPerSegment` 时跨输入继续合并；追加下一个最多 1000 行的源 segment 后，`splitColumn` 会立即按 1000 行切分，因此输出 segment 最多 1000 行，临时 `c.col` 严格少于 2000 行。
   - HTTP 行协议由 `max-line-size` 限制完整单行长度。代码默认值为 1MiB，仓库示例配置为 64KiB；String field 解码只去除引号/转义，其单列值字节数不会大于所在行的输入字节数。
   - 即使按 1MiB 上限估算，1000 行输出 segment 的单个 String field `ColVal.Val` 不超过约 1000MiB，临时合并对象低于约 2000MiB，均小于 uint32 的 4GiB 表示边界。因此本次不为 stream compact 增加 rows + bytes 条件，也不在 bytes 达阈值时提前 `writeSegment`。
   - 该结论以 `max-rows-per-segment=1000`、`max-line-size<=1MiB` 且数据经过行协议限制为配置前提。未来若提高任一上限或引入绕过该限制的写入入口，应单独增加配置组合校验或重新评估 stream compact；不纳入本次修改。
2. stream merge 的最终输出同样不需要 rows + bytes 切分，但按 ordered 当前 segment 时间范围读取 unordered、以及最后一次读取剩余全部 unordered 时，单次 `Read(maxTime)` 可能覆盖多个 segment。本方案不改变现有 `Handle -> readUnordered -> merge -> columnWriter` 主流程，只给 time range 读取增加 `rowsLimit`，默认每批最多 1000 行并循环处理；不改写 `columnWriter`，也不引入 `MaxVarColValBytes` 驱动的输出切分。
3. `decodeColumnData` / `DecodeStringBlock` / `unpackStringV1/V2` 收到的是 segment 级编码块。在上述前提下，它不是本次跨 segment 累积溢出的主修点。
4. TSStore 时序写入路径中，tag 不作为数据列 append 到持久化 `Record.ColVals`。`tsMemTableImpl.WriteRows` 只把 `row.Fields` 传给 `appendFields` / `AppendFieldsToRecord`；tag 通过 series key / `tagSets` 参与 series 定位、过滤和结果附加信息。
5. 查询侧按 series 迭代数据。`shard.createGroupCursors` 接收 `tagSets` 创建 group / series cursor，`seriesCursor.ReInit` 将 `TagSetItem.TagsVec` 放入 `seriesInfo`，tag filter 从 `PointTags` 取值，不依赖 TSSP 数据列。
6. 查询结果中的 aux tag 可在 `tagSetCursor.TagAuxHandler` 中 append 到结果 `Record`，但它不是 TSStore 落盘、snapshot、compact、merge 的数据路径，不作为本方案主保护对象。
7. 不支持单个 String field 值超过产品写入上限。超限单值应在 memtable mutation 前拒写。
8. 线格式保持不变。`ColVal.Offset`、TSSP string block offset/length、`Segment.size`、`ChunkMeta.size` 仍保持现有 uint32 表达。
9. 不把 `ColVal.Offset` 改成 uint64。`ColVal` 是共享核心结构，改类型会扩大到大量读写边界；本方案采用 bounded append、segment/rowsLimit 有界处理和流程降级控制风险。

---

## 二、核心策略

### 0. 两个数据大小维度

本问题可以拆成两个不同粒度的数据大小维度:

| 维度 | 对应字段 | 含义 | 本方案定位 |
|------|----------|------|------------|
| 单列变长数据大小 | `ColVal.Offset` | String field 单列的 `ColVal.Val` 字节大小；`Offset` 记录每个值在该列 `Val` 中的位置 | 主保护对象，通过 append 前预算或 segment/time+rowsLimit 有界处理避免回绕 |
| 单 chunk 数据大小 | `ChunkMeta.size` | 一个 `ChunkMeta` 描述的 chunk data 区大小，包含同一 series chunk 内 time 列和各 field 列的 segment 数据 | 输出侧维持现状；读、compact、merge 侧把它视为不可信元数据 |

在当前 TSStore TSSP 组织下，一个 `ChunkMeta` 对应一个 series 在一个 TSSP 文件中的一个 chunk data 区，通常可理解为该 series 在该文件内的数据块。若未来同一 series 在同一文件内被拆成多个 chunk meta，本维度仍按“单 chunk”计算，而不是按全文件或全 measurement 计算。

这两个维度不能相互替代:`ChunkMeta.size` 描述的是落盘后的 chunk data 字节范围，通常是压缩/编码后的数据；`ColVal.Offset` 描述的是 decode 后单列 `ColVal.Val` 内部的偏移。即使 `ChunkMeta.size` 看起来可信，decode 后的 String field `ColVal.Offset` 仍可能越界，必须单独校验。

### 1. 主保护对象:`ColVal.Offset`

真正必须阻止的是 String field `ColVal.Val` 在内存中跨 record / segment 无界累积到 uint32 上限以上。此类路径必须在 mutation 前做 bytes 预算，或把单次处理行数严格限制住。stream compact 由 segment 行数约束，stream merge 由 time range + `rowsLimit` 分批读取约束；两者在当前“每批最多 1000 行 + 每行最多 1MiB”的配置前提下形成低于 uint32 上限的独立边界，不纳入 `MaxVarColValBytes` 切分。

核心不变量:

> 除上述按 rows + `max-line-size` 已有界的 stream compact 和 stream merge rowsLimit batch 外，任意会跨 record / segment 累积的 TSStore String field 列追加到目标 `ColVal` 前，必须保证追加后 `len(dst.Val) <= MaxVarColValBytes`。

建议阈值:

- `MaxVarColValBytes`:默认 256MiB，可配置到 512MiB。
- 测试阈值:支持 1KiB / 64KiB 等小阈值，用于验证 flush、split、降级和 fail-closed 行为。

### 2. `ChunkMeta.size` 策略:输出将错就错，读侧不可信

`ChunkMeta.size` 是 uint32，表示一个 series chunk 的 data 区总长度。当前依赖点不多，本方案不把它作为输出侧强保护目标:

- compact / merge 等后台流程写新文件时，`ChunkMeta.size` 维持现状，仍可能按 uint32 回绕写入。
- 不引入 `MaxChunkDataBytes` 作为必须切分新 chunk 的硬约束。
- 但所有后续流程不得无条件相信源 `ChunkMeta.size` 是真实 chunk data 长度。

判断源 `ChunkMeta.size` 是否可信时，先计算不依赖 `cm.size` 的 `expectedChunkDataSize`。实际落地可按场景选择算法:

1. 相邻 chunk offset 差值:
   - 若能拿到同一文件中下一个 chunk meta，使用 `nextChunkMeta.offset - currentChunkMeta.offset`。
   - 该方式能直接得到当前 chunk data 到下一个 chunk data 起点之间的真实跨度，适合 chunk meta 顺序完整、相邻 chunk 可访问的场景。
2. segment entry 覆盖范围:
   - 使用 `max(entry.offset + entry.size) - cm.offset`。
   - 该方式不依赖下一个 chunk meta，适合单 chunk 校验、最后一个 chunk、或只拿到当前 chunk meta 的场景。

若 `expectedChunkDataSize > int64(cm.size)`，说明源 `ChunkMeta.size` 已截断、回绕或元数据损坏。此时:

- 非流式 compact / merge 不能继续用 `cm.size` 整 chunk 读取。
- 查询路径不能继续依赖 `cm.size` 做整 chunk 预读。
- `WriteOriginal` 不能用 `meta.size` 作为 fast-copy 的复制长度。

### 3. 重要约束:writer 内部 offset 不能依赖回绕 size

虽然输出的 `ChunkMeta.size` 可以将错就错，但 writer 内部推进 offset 不能依赖已回绕的 `chunkMeta.size`。否则会把后续 chunk 的 `offset` 写错，问题就不再只是 size metadata 不准，而是文件布局损坏。

因此:

- stream compact / stream merge 当前主要用 `writer.DataSize()` 定位新 chunk，方向是合理的。
- snapshot / flush builder 中如使用 `chunkMeta.size` 推进 `dataOffset`，需要改为使用实际编码/写入字节数。

---

## 三、模块风险总览

| 模块 | 主要流程 | 风险点 | 处理原则 |
|------|----------|--------|----------|
| 写入 / memtable | 行协议解析 -> record append -> memtable mutation | String field `ColVal` 持续累积，append 前无 bytes 预算会形成回绕 offset | mutation 前预算，超限 flush/retry 或拒写 |
| WAL | shard `binaryRows` -> snappy physical record -> WAL header | 在现有行协议单行限制和写入批量约束下，单个 payload 不可能达到 uint32 表示边界 | 本次不修改 WAL，也不增加 payload 长度比较 |
| snapshot / flush | memtable record -> `MsBuilder` -> TSSP chunk | 只按 rows 切分不足以约束 String field bytes；内部 `dataOffset` 不能依赖回绕的 `chunkMeta.size` | rows + var-bytes 切分；offset 用真实写入大小推进 |
| 非流式 compact / merge fastmode | chunk 整块读取 -> decodeRecord -> record merge -> 写新文件 | 整 chunk 读取依赖源 `ChunkMeta.size`；decode 后 `ColVal.Offset` 仍可能越界；record merge 会继续累积 `ColVal` | 源 size 不可信或 decode 后 offset 越界时降级 streamMode；merge 使用 bounded append |
| stream compact | segment 读取 -> compactColumn -> writeSegment -> writeMeta | 源读按 segment 安全；当前 rows 上限与 `max-line-size` 组合已保证输出 segment 和临时合并对象低于 uint32 边界 | 保持 row-only 合并与现有 `writeSegment` 时机；输出 `ChunkMeta.size` 维持现状 |
| stream merge | ordered/unordered segment merge 或 `WriteOriginal` fast-copy | ordered 输入按 segment 有界，但按当前 segment 时间范围或 `math.MaxInt64` 读取 unordered 时可一次聚合多个 segment；`WriteOriginal` 当前信任源 `meta.size` | 保留现有 merge / `columnWriter` 流程；unordered time range 读取增加最多 1000 行的 `rowsLimit` 并循环消费；fast-copy 长度改用 next chunk offset 或 segment entry 覆盖范围 |
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
- `SplitByRowsAndVarBytes`
- `ValidateCol`
- `ValidateRecord`

错误语义:

- `ErrNeedFlush`:追加本身合法，但目标对象接近阈值，需要调用方先 flush/split/writeSegment 后重试。
- `ErrValueTooLarge`:单值或不可拆单次追加超过产品上限，拒写。
- `ErrCorruptColumn`:源 offset 或源 meta 不可信，不能继续 append 或 fast-copy。

### 2. 源 chunk size 校验

新增 `ValidateChunkMetaDataRange(cm *ChunkMeta, next *ChunkMeta, fileDataSize int64) (expectedSize int64, err error)`:

- 优先在可取得 `next` 时使用 `next.offset - cm.offset` 计算 expected size。
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

- 当前切分以 rows 为主，不能限制 String field `ColVal.Val` 字节增长。
- `ChunkMeta.size` 输出可以将错就错，但 builder 内部不能使用回绕后的 `chunkMeta.size` 推进 `dataOffset`。

#### 设计动作

1. snapshot 写出前使用 `SplitByRowsAndVarBytes`:
   - 按 rows 和 String field bytes 双条件切分。
   - 不再把超阈值 record 交给 builder。
2. `ChunkDataBuilder` 维持当前 `ChunkMeta.size uint32` 输出行为，不因为 chunk data 超 4GB 强制切 chunk。
3. `MsBuilder` 推进 `dataOffset` 时使用实际编码/写入字节数，例如 `len(encodeChunk)` 或 writer 实际 `DataSize` 差值，而不是 `chunkBuilder.chunkMeta.size`。

#### 验收点

- 低 `MaxVarColValBytes` 下，snapshot 会切成多个 bounded record。
- 构造 chunk data size 回绕场景时，后续 chunk offset 仍按真实写入位置推进。
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
6. 对大 String field 场景，优先绕开整 record fast path，使用按 segment 或 time + `rowsLimit` 分批读取的流式路径；stream compact 保持现有 row-only 边界，stream merge 保持现有 merge / `columnWriter` 流程并限制单次 unordered 读取行数。

#### 验收点

- 源 `ChunkMeta.size` 小于 expected size（由 next chunk offset 或 segment entry 计算）时，非流式 compact / merge 降级 streamMode。
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

#### 调研结论与风险点

- `ColumnIterator.walkSegment` 传给 `mergePerformer.Handle` 的 ordered 列是单 segment，最多 `maxRowsPerSegment` 行；`StreamWriteFile.WriteData` 也断言最终输出列不超过该行数。
- 风险集中在两种 unordered time range 读取:
  - 普通 `Handle` 使用 ordered 当前 segment 的 `maxOrderTime` 调用 `readUnordered(maxOrderTime)`。若该时间范围内 unordered 点密度远高于 ordered，一个 segment 的时间范围仍可能命中多个 unordered segment。
  - `lastSeries && lastSeg` 会把 `maxOrderTime` 提升到 `math.MaxInt64`，当前实现可能一次读取该 series 剩余的全部 unordered 数据。
- `UnorderedColumnReader.read` 会循环读取 source segment 直到满足 `need`；如果 `need` 来自不受行数限制的 `ReadTimes(maxTime)`，会在进入 `MergeHelper` 和 `columnWriter` 前形成大 `ColVal`。
- `columnWriter` 的最终 row-only split 本身没有问题，也不是本次改造点。只要传给它的每个 merged batch 有明确行数上限，现有 `remain -> splitRemain -> WriteData` 流程可以继续使用。
- `WriteOriginal` 当前使用 `for readSize < meta.size` 复制原 chunk；源 `meta.size` 已回绕时会复制截断数据并生成坏新文件。
- `StreamWriteFile.WriteMeta` 输出 `ChunkMeta.size` 当前会 uint32 窄化，本方案不强制保护该输出。

#### 核心不变量

设 `R = maxRowsPerSegment`，当前为 1000；`unorderedReadRowsLimit` 默认直接取 `R`，不新增独立可调参数。

> `ReadTimes(maxTime, rowsLimit)` 每次最多推进 `rowsLimit` 个全局去重后的 unordered timestamp；所有 reader 只读取到本批最后一个 timestamp，并在返回 `hasMoreWithinRange=true` 时由调用方继续循环。不得用原始 `maxTime` 让单个 reader 越过本批边界。

按 `rowsLimit = R` 计算:

- `UnorderedColumnReader` 的旧 tail 小于一个 source segment，本批最多再读取到满足 `R` 行，因此单个 reader 的临时列严格小于 `2R` 行。
- 当前 ordered segment 最多 `R` 行；按批次时间边界切出的 ordered range 与 unordered batch 合并后最多 `2R` 行。
- `columnWriter.remain` 在调用前小于 `R` 行，append 本批 merged col 后临时对象严格小于 `3R` 行，随后沿用现有 `splitRemain` 按 `R` 行写出。
- 在 `R=1000`、`max-line-size<=1MiB` 的前提下，最坏临时 String field `ColVal.Val` 低于约 3000MiB，仍小于 uint32 的 4GiB 边界；因此不需要 rows + bytes 条件。

#### 设计动作

1. 为现有 time range API 增加行数上限和续读状态，建议签名:

   ```go
   ReadTimes(maxTime int64, rowsLimit int) (times []int64, hasMoreWithinRange bool, err error)
   Read(sid uint64, maxTime int64, rowsLimit int) (col *record.ColVal, times []int64, hasMoreWithinRange bool, err error)
   ```

   - 先按 `maxTime` 查找本次允许读取的 `logicalEnd`，再令 `batchEnd = min(currentOffset + rowsLimit, logicalEnd)`，只推进并返回 `[currentOffset, batchEnd)`；`hasMoreWithinRange = batchEnd < logicalEnd`。
   - 实际传给各 `UnorderedColumnReader.Read` 的上界必须是本批 `times[len(times)-1]`，不能继续使用调用方传入的原始 `maxTime`。
   - `hasMoreWithinRange` 只表示当前 `maxTime` 范围内仍有未消费 timestamp；调用方据此继续循环。
   - `rowsLimit <= 0` 视为内部参数错误；正常路径统一传 `GetMaxRowsPerSegment4TsStore()`。
   - source segment decode 后执行 `ValidateCol`；offset/length 不可信时返回 corrupt error。
2. ordered 当前 segment 范围内读取 unordered 时，保留 `Handle -> readUnordered -> merge -> write` 结构，只在 `Handle` 内增加批次循环:
   - `readUnordered(maxOrderTime, R)` 返回最多 `R` 行 unordered 及 `hasMoreWithinRange`。
   - 以本批最后一个 unordered timestamp 为边界，在当前 ordered segment 中用 `sort.Search` 找到 `<= batchMaxTime` 的连续 range；只把该 ordered range 与本批 unordered 交给现有 `MergeHelper.Merge`。
   - 非最终 batch 调用现有 `p.write(..., lastSeg=false)`，推进 ordered range 起点后继续读取下一批；当前 `maxOrderTime` 范围内 unordered 耗尽后，再写剩余 ordered range。
   - timestamp 等于批次边界的 ordered/unordered 点必须在同一批处理，继续保持 unordered 覆盖 ordered，不能把同 timestamp 拆到下一批。
   - 原始 `lastSeg=true` 只传递一次：如果最后一个 unordered batch 已同时消费完 ordered range，就传给该 batch；否则传给最后的 ordered suffix。若范围内没有 unordered，则直接沿用现有 ordered-only 写法。这样不需要构造空 batch，且 `columnWriter.flush` 时机不变。
3. 读取剩余全部 unordered 时继续使用现有分支语义，但把“全部”改为循环批次:
   - `lastSeries && lastSeg` 仍可把逻辑上界设为 `math.MaxInt64`，但每次 `Read` 最多返回 `R` 行；当 ordered range 已消费完，后续批次以空 ordered col 与 unordered batch 继续走现有 merge/write 流程。
   - `writeUnorderedCol` 不再为整个 `p.mergedTimes` 一次构造 nil col；将 `p.mergedTimes` 按最多 `R` 行切成连续 range，为每个 range 构造 nil col，并以该 range 的最后时间调用带 `rowsLimit` 的 `Read`。由于 range 本身最多 `R` 个全局 timestamp，本次读取必须返回 `hasMoreWithinRange=false`，随后沿用现有 merge/write，最后一个 range 才传 `lastSeg=true`。
   - `UnorderedReader.readRemain` 已按 `maxRowsPerSegment` 推进 `wn`，保留该流程，只改为显式传入 `rowsLimit` 并断言单批返回不超过 `R` 行。
4. `columnWriter.write`、`splitRemain`、`flush` 和 `writeMergedTime` 保持现状:
   - 不新增 rows + bytes 条件或新的写出抽象，不改变现有 segment packing、pre-aggregation 和 time 列写出流程。
   - 在 `columnWriter.write` 入口增加 debug/invariant 统计即可，验证输入 merged batch 不超过 `2R`、append 后 `remain` 不超过安全推导边界；违反时 fail-closed，不回退到全量读取。
5. 分批前后必须保持现有语义:
   - 输出 timestamp 全局有序，批次之间无遗漏、重复或逆序。
   - ordered/unordered timestamp 相同时仍由 unordered 值覆盖；多个 unordered file 同 timestamp 的优先级保持当前 reader 顺序。
   - 各 field 在 `ColumnChanged` 后从相同 unordered time offset 开始，基于相同 `maxTime + rowsLimit` 规则得到相同批次边界；nil bitmap、行数和 time 列继续对齐。
   - rowsLimit 只限制内存中的单次读取/归并，不改变 TSSP 线格式，也不成为用户可配置的 segment 规则。
6. `WriteOriginal` 不再依赖源 `meta.size`:
   - 按场景使用 `nextChunkMeta.offset - meta.offset` 或 segment entry 覆盖范围计算 `expectedChunkDataSize`。
   - fast-copy 复制范围使用 `[meta.offset, meta.offset + expectedChunkDataSize)`。
   - 分块读取时每次仍可用较小 uint32 buffer size，但循环总长度用 int64。
   - 平移 segment offset 后写新 meta；新 `ChunkMeta.size` 仍维持当前 uint32 输出行为。
7. 如果 segment entry range 本身不可信，禁用 `WriteOriginal`，改走逐 segment rewrite；无法安全重写时返回 corrupt error。

#### 复杂度与性能边界

- 不预生成新的全列分批计划，不改 `ColumnIterator`、`columnWriter` 和 time 列主流程；新增工作只是在单次 time range 命中超过 `R` 行时多次调用现有 read/merge/write。
- 每个 unordered timestamp 仍只被当前 field 的 offset 单调消费一次，整体时间复杂度保持线性；批次数约为 `ceil(matchedUnorderedRows / R)`。
- `columnWriter` 继续按 `R` 行打包最终 segment，因此最终 TSSP segment 数由合并后总行数决定，不因 unordered 读取分批而额外增加。
- `mergedTimes` 及现有 schema/reader 生命周期保持不变；本方案只限制 String 列单次 materialize 的行数。

伪流程:

```text
Handle(ordered current segment)
  -> maxTime = ordered segment maxTime 或 math.MaxInt64
  -> 循环 Read(maxTime, rowsLimit=1000)
       -> 得到本批 unordered 和 batchMaxTime
       -> 切出 ordered 中 <= batchMaxTime 的 range
       -> 走现有 MergeHelper.Merge
       -> 非最终批走现有 columnWriter.write(lastSeg=false)
       -> 若本批已耗尽 unordered 和 ordered，则沿用原 lastSeg
  -> 若仍有 ordered suffix，最后写 suffix 并沿用原 lastSeg/flush
```

#### 验收点

- ordered 当前 segment 时间范围内包含超过 `R` 行 unordered 时，触发多批 `Read(maxOrderTime, R)`；输出与改造前全量 merge 结果逐行一致。
- `lastSeries && lastSeg` 后仍有多个 unordered segment 时，以 `math.MaxInt64 + rowsLimit` 多批排空，不产生一次性全量 String `ColVal`。
- ordered batch 边界与 unordered timestamp 相等时，两侧在同一批合并且 unordered 仍覆盖 ordered；多个 unordered file 的覆盖优先级不变。
- ordered-only、unordered-only、字段仅存在于一侧、nil bitmap、pre-aggregation 和 time/field 总行数均保持现有语义。
- 使用小 `R` 构造 source segment tail + 多批读取，验证单 reader 临时列 `<2R`、merged batch `<=2R`、`columnWriter` append 后临时对象 `<3R`。
- `columnWriter.write` / `splitRemain` / `flush` 的既有测试结果不变；低 `MaxVarColValBytes` 不改变 stream merge 的 rowsLimit 或 segment 边界。
- 源 `meta.size` 回绕时，`WriteOriginal` 不再只复制前 `meta.size` 字节。
- 源 segment entry range 不可信时，不走 fast-copy。

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
| `mergePerformer.WriteOriginal` | 当前依赖，风险最高 | 改为按 next chunk offset 或 segment entry 覆盖范围复制，不使用源 `meta.size` |
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

- 新增 `ValidateChunkMetaDataRange`，支持用 next chunk offset 或 segment entry 覆盖范围判断源 `ChunkMeta.size` 是否可信。
- 查询整 chunk 预读失败或校验不通过时，降级到 per-segment read。
- 非流式 compact / merge fastmode 发现源 `ChunkMeta.size` 不可信，或整 chunk decode 后 `ColVal.Offset` 越界时，降级 streamMode。
- `WriteOriginal` 复制长度改为依赖 next chunk offset 或 segment entry 覆盖范围，而不是源 `meta.size`。
- `appendFields` 前按整次 shard batch 汇总 String field bytes 并校验单值；预算失败不产生部分 data mutation，也不做 mutation 回滚。
- 单值超限拒写；追加会使 memtable `ColVal` 超阈值时返回 `ErrNeedFlush`，触发 snapshot/flush 后重试。
- 引入 String field 判断、bounded append API 和 rows + var-bytes splitter。
- 除已有 row-only 边界的 stream compact，以及按 `maxTime + rowsLimit` 分批读取的 stream merge 外，高风险 `AppendColVal`、`AppendString`、直接 `uint32(len(cv.Val))` 统一收敛到 bounded API。
- snapshot/flush 按 rows + var-bytes 切分。
- snapshot/flush 内部 offset 推进改为真实写入大小，不依赖回绕 `chunkMeta.size`。
- stream merge 保留现有 `Handle -> readUnordered -> merge -> columnWriter` 主流程；按 ordered 当前 segment 的 `maxOrderTime` 读取、以及用 `math.MaxInt64` 排空剩余 unordered 时，均增加 `rowsLimit=1000` 并循环消费。
- 非流式 compact / merge 大 String field 场景降级到按 segment 或 `maxTime + rowsLimit` 分批读取的 streamMode；stream compact 保持现有 row-only 切分，stream merge 保持现有 merge / `columnWriter` 流程。
- compact / merge 输出 `ChunkMeta.size` 维持现状，不作为本方案保护目标。

---

## 七、测试策略

### 写入 / snapshot

- 低 `MaxVarColValBytes` 下，同一 series String field 列接近阈值后继续写入，验证 mutation 前返回 `ErrNeedFlush`。
- 单值超过产品上限，验证不产生 memtable mutation。
- 同一 batch 构造多个 series / String field，其中后处理字段预算失败，验证前面的字段也没有发生 mutation。
- snapshot 只按行数不足以切开的场景，验证 rows + bytes splitter 生效。
- 构造 `ChunkMeta.size` 回绕场景，验证 snapshot writer 内部后续 chunk offset 仍按真实写入大小推进。

### `ChunkMeta.size` 读取降级

- 构造 `ChunkMeta.size` 小于 expected size 的源 meta，分别覆盖 next chunk offset 和 segment entry 两种算法，验证非流式 compact / merge 降级 streamMode。
- 构造 `ChunkMeta.size` 可信但 decode 后 String field `ColVal.Offset` 越界的整 chunk 数据，验证非流式 compact / merge fastmode 降级 streamMode。
- 查询路径触发 `defaultIoSize` 整 chunk 预读但 decode/range check 失败时，验证降级 per-segment read。
- segment entry 本身不可信时，验证查询/compact 返回 corrupt error，不 panic。
- stream compact / stream merge 输出回绕 `ChunkMeta.size` 后，后续 streamMode 仍可按 segment entry 读取。

### Compact / merge

- 非流式 compact / merge 的 record 合并总 bytes 超低阈值时，验证不会构造超阈值目标 `ColVal`。
- stream compact 复用现有 rows 边界测试，不新增低 `MaxVarColValBytes` 下提前 `writeSegment` 的测试预期。
- stream merge 在单个 ordered segment 时间范围命中超过 1000 行 unordered 时，多次调用 `Read(maxOrderTime, rowsLimit=1000)`；每批 unordered 不超过 1000 行，最终输出与改造前逐行一致。
- 最后一个 ordered segment 使用 `math.MaxInt64` 排空剩余 unordered 时同样按 1000 行循环读取，不一次构造剩余全部 String `ColVal`；unordered-only 字段按 `mergedTimes` 的连续 1000 行 range 处理。
- 多个 unordered file 在批次边界存在相同 timestamp 时，验证同 timestamp 不跨批、覆盖优先级、nil bitmap、time/field segment 边界与改造前一致。
- 低 `MaxVarColValBytes` 不改变 stream merge 的 `rowsLimit` 或最终 segment 边界，也不触发 bytes 阈值提前 flush/split。
- 源 `meta.size` 回绕时，`WriteOriginal` 仍按 next chunk offset 或 segment entry 覆盖范围复制，不生成截断新文件。

### 兼容性

- 旧 TSSP 新二进制可读。
- 新二进制写出的 TSSP 仍保持旧格式，旧二进制可读。
- 含坏 offset / 坏 chunk meta 的旧文件，新二进制读不 panic。

---

## 八、改动清单

### 共享 record 能力

- `lib/record/column.go`:String field 判断、checked accessor、bounded append 入口。
- `lib/record/record_check.go`:新增 `ValidateCol` / `ValidateRecord`。
- `lib/record/*`:新增 rows + var-bytes splitter。

### 写入 / memtable

- `engine/mutable/ts_table.go`:按整次 shard batch 汇总 String field bytes，在任何字段 append 前完成预算；预算与 mutation 使用同一同步域或 reservation。
- `engine/shard.go`:处理 `ErrNeedFlush` / `ErrValueTooLarge`；调整 mem size/token 时序，失败只释放资源，不回滚 data mutation。
- `lib/errno`:新增或统一 `ErrNeedFlush` / `ErrValueTooLarge` 的错误语义。

### Snapshot / flush

- `engine/immutable/msbuilder.go`:写 record 前按 rows + var-bytes 切分；`dataOffset` 用真实写入大小推进。
- `engine/immutable/chunkdata_builder_ts.go`:保留 `ChunkMeta.size uint32` 输出现状，不新增 chunk byte cap。
- `engine/immutable/column_builder.go`:保留 segment 写前断言。

### Compact

- `engine/immutable/chunk_iterators.go`:整 chunk 读取前校验源 chunk data range；整 chunk decode 后校验 `ColVal.Offset`；不可信或 offset 越界时触发 streamMode 降级。
- `engine/immutable/stream_compact.go`:本次不修改 `compactColumn` / `continueMerge` / `writeSegment`；`writeMetaToDisk` 的 `ChunkMeta.size` 输出维持现状。
- `engine/immutable/merge_tool.go` / `merge_self.go`:大 String field、源 `ChunkMeta.size` 不可信，或整 chunk decode 后 `ColVal.Offset` 越界时绕开整 record fast path。

### Merge

- `engine/immutable/merge_performer.go`:保留现有按 ordered segment 回调及 `merge -> columnWriter` 写出流程；在当前 segment 的 `maxOrderTime` 和最后排空用的 `math.MaxInt64` 范围内，循环读取最多 `maxRowsPerSegment` 行 unordered，并按本批最后时间点切分 ordered range；`WriteOriginal` 复制长度改用 next chunk offset 或 segment entry 覆盖范围，不依赖源 `meta.size`。
- `engine/immutable/stream_downsample.go`:`StreamWriteFile.WriteMeta` 的 `ChunkMeta.size` 输出维持现状。
- `engine/immutable/unordered_reader.go`:为 `ReadTimes` / `Read` 增加 `rowsLimit` 和 `hasMoreWithinRange`；先从全局 unordered times 截取最多 `rowsLimit` 行，再以本批最后时间点限制每个 source reader，避免按任意大的原始 `maxTime` 聚合完整 String 列。
- `engine/immutable/column_iterator.go`:保持 ordered 数据按 segment 回调，不修改读取与回调流程。

### 查询

- `engine/immutable/tssp_file_meta.go` / `chunk_meta_codec.go`:chunk data range 校验辅助。
- `engine/immutable/tssp_file.go`:整 chunk 预读失败或 size 不可信时降级 per-segment read；`columnData` 前增加边界判断。

---

> 第二步的灰度开关、放量、回滚、运行监控与总体决策摘要见 [`uint32-offset-overflow-fix-phase2-design.md`](uint32-offset-overflow-fix-phase2-design.md)。
