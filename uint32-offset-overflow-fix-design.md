# TSStore uint32 溢出修复方案设计

> 关联文档:
> - `uint32-offset-overflow-panic-analysis-tsstore.md`
> - `uint32-offset-overflow-fix-codex-dialogue.md`
>
> 目标:在不改变 TSSP/WAL 线格式的前提下，消除 TSStore 中 String field 列跨 record、segment 累积后导致的 `ColVal.Offset` 回绕、数组越界 panic 和坏数据落盘；同时把 `ChunkMeta.size` 作为不可信元数据处理，避免读/compact/merge 依赖已回绕的 chunk size。

---

## 一、范围与前提

只考虑 TSStore（ts）路径。ColumnStore（cs）、colstore compact、detached primary key/data/index 等路径不在本方案范围内。

明确前提:

1. 不考虑单 segment 超过 4GB。行协议受 `max-line-size` 限制，TSStore 默认单 segment 行数有限，本方案不为“1000 个点组成的单 segment 超过 4GB”设计主线。
2. `decodeColumnData` / `DecodeStringBlock` / `unpackStringV1/V2` 收到的是 segment 级编码块。在上述前提下，它不是本次跨 segment 累积溢出的主修点。
3. TSStore 时序写入路径中，tag 不作为数据列 append 到持久化 `Record.ColVals`。`tsMemTableImpl.WriteRows` 只把 `row.Fields` 传给 `appendFields` / `AppendFieldsToRecord`；tag 通过 series key / `tagSets` 参与 series 定位、过滤和结果附加信息。
4. 查询侧按 series 迭代数据。`shard.createGroupCursors` 接收 `tagSets` 创建 group / series cursor，`seriesCursor.ReInit` 将 `TagSetItem.TagsVec` 放入 `seriesInfo`，tag filter 从 `PointTags` 取值，不依赖 TSSP 数据列。
5. 查询结果中的 aux tag 可在 `tagSetCursor.TagAuxHandler` 中 append 到结果 `Record`，但它不是 TSStore 落盘、snapshot、compact、merge 的数据路径，不作为本方案主保护对象。
6. 不支持单个 String field 值超过产品写入上限。超限单值应在进入 memtable / WAL 前拒写。
7. 线格式保持不变。`ColVal.Offset`、TSSP string block offset/length、`Segment.size`、`ChunkMeta.size`、WAL physical header 仍保持现有 uint32 表达。
8. 不把 `ColVal.Offset` 改成 uint64。`ColVal` 是共享核心结构，改类型会扩大到大量读写边界；本方案采用 bounded append 和流程降级控制风险。

---

## 二、核心策略

### 0. 两个数据大小维度

本问题可以拆成两个不同粒度的数据大小维度:

| 维度 | 对应字段 | 含义 | 本方案定位 |
|------|----------|------|------------|
| 单列变长数据大小 | `ColVal.Offset` | String field 单列的 `ColVal.Val` 字节大小；`Offset` 记录每个值在该列 `Val` 中的位置 | 主保护对象，必须通过 append 前预算避免回绕 |
| 单 chunk 数据大小 | `ChunkMeta.size` | 一个 `ChunkMeta` 描述的 chunk data 区大小，包含同一 series chunk 内 time 列和各 field 列的 segment 数据 | 输出侧维持现状；读、compact、merge 侧把它视为不可信元数据 |

在当前 TSStore TSSP 组织下，一个 `ChunkMeta` 对应一个 series 在一个 TSSP 文件中的一个 chunk data 区，通常可理解为该 series 在该文件内的数据块。若未来同一 series 在同一文件内被拆成多个 chunk meta，本维度仍按“单 chunk”计算，而不是按全文件或全 measurement 计算。

这两个维度不能相互替代:`ChunkMeta.size` 描述的是落盘后的 chunk data 字节范围，通常是压缩/编码后的数据；`ColVal.Offset` 描述的是 decode 后单列 `ColVal.Val` 内部的偏移。即使 `ChunkMeta.size` 看起来可信，decode 后的 String field `ColVal.Offset` 仍可能越界，必须单独校验。

### 1. 主保护对象:`ColVal.Offset`

真正必须阻止的是 String field `ColVal.Val` 在内存中跨 record / segment 累积到 uint32 上限以上。所有会把 String field 追加到目标 `ColVal` 的路径，都必须在 mutation 前做 bytes 预算。

核心不变量:

> 任意 TSStore String field 列追加到目标 `ColVal` 前，必须保证追加后 `len(dst.Val) <= MaxVarColValBytes`。

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
| WAL | batch -> snappy physical record -> WAL header | WAL 长度失败不能晚于 memtable mutation 暴露 | WAL 长度前置检查，拆 batch 或拒写 |
| snapshot / flush | memtable record -> `MsBuilder` -> TSSP chunk | 只按 rows 切分不足以约束 String field bytes；内部 `dataOffset` 不能依赖回绕的 `chunkMeta.size` | rows + var-bytes 切分；offset 用真实写入大小推进 |
| 非流式 compact / merge fastmode | chunk 整块读取 -> decodeRecord -> record merge -> 写新文件 | 整 chunk 读取依赖源 `ChunkMeta.size`；decode 后 `ColVal.Offset` 仍可能越界；record merge 会继续累积 `ColVal` | 源 size 不可信或 decode 后 offset 越界时降级 streamMode；merge 使用 bounded append |
| stream compact | segment 读取 -> compactColumn -> writeSegment -> writeMeta | 源读按 segment 安全；合并条件仍需 bytes 边界 | rows + bytes merge 条件；输出 `ChunkMeta.size` 维持现状 |
| stream merge | ordered/unordered segment merge 或 `WriteOriginal` fast-copy | `columnWriter` 可能累积过大；`WriteOriginal` 当前信任源 `meta.size` | remain byte-bounded；fast-copy 长度改用 next chunk offset 或 segment entry 覆盖范围 |
| 查询 / 诊断 | chunk meta -> segment data -> decode | `defaultIoSize` 整 chunk 预读依赖 `cm.size`；历史坏 offset 不能 panic | 预读失败/校验不通过时降级按 segment 读；fail-closed |

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

### 1. 写入、WAL、memtable

#### 风险点

- memtable 中同一 series 的 String field `ColVal` 会跨多次写入持续增长。
- append 过程中不能出现部分列已 mutation、后续列因超限失败的半写状态。
- WAL physical record 使用 uint32 长度字段，长度失败必须在 memtable mutation 前暴露。

#### 设计动作

1. 在 `appendFields` / `AppendFieldsToRecord` 前做 String field bytes 预算。
2. 预算覆盖整次 batch，确保不会发生部分字段 append 后失败。
3. 单值超过产品上限返回 `ErrValueTooLarge`。
4. 追加会使目标 `ColVal` 超过 `MaxVarColValBytes` 时返回 `ErrNeedFlush`，触发 snapshot/flush 后重试。
5. WAL physical record 长度在 memtable mutation 前检查，超限时拆 batch 或拒写。
6. `AddMemSize` / token 最好放在预算成功之后；若保持现有顺序，错误返回时必须回滚。

#### 验收点

- 低阈值下，同一 series String field 列接近阈值后继续写入，会在 mutation 前返回 `ErrNeedFlush`。
- 单值超限不写 memtable、不写 WAL。
- WAL 超限 batch 不会出现 memtable 成功但 WAL 失败。

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
6. 对大 String field 场景，优先绕开整 record fast path，使用 byte-bounded 流式路径。

#### 验收点

- 源 `ChunkMeta.size` 小于 expected size（由 next chunk offset 或 segment entry 计算）时，非流式 compact / merge 降级 streamMode。
- 整 chunk 读取后 decode 出的 String field `ColVal.Offset` 越界时，非流式 compact / merge 降级 streamMode。
- 降级后不发生 `columnData` slice panic。
- 多 segment 合并超过低阈值时，输出多个 bounded record。

### 4. Stream compact

#### 风险点

- 源读取按 segment offset/size 进行，不依赖源 `ChunkMeta.size` 定位数据。
- `compactColumn` / `continueMerge` 如果只按 rows 判断，仍可能让当前 `c.col` 的 String field bytes 持续增长。
- 输出 `ChunkMeta.size` 当前在 `writeMetaToDisk` 中用 `uint32(writer.DataSize() - cm.offset)` 写入，本方案不强制改为防回绕。

#### 设计动作

1. `continueMerge` 从 row-only 改成 rows + bytes 双条件。
2. 从 `lastSeg`、`tmpCol` append 到 `c.col` 前做 String field bytes 预算。
3. 达到阈值时先 `writeSegment`，再追加新 segment。
4. `writeMetaToDisk` 的输出 `ChunkMeta.size` 维持现状；后续读取方不得依赖它作为真实 chunk data 长度。

#### 验收点

- 低 `MaxVarColValBytes` 下，stream compact 会提前 `writeSegment`。
- 即使输出 `ChunkMeta.size` 回绕，后续 stream 读取仍可按 segment entry 读取。

### 5. Stream merge / out-of-order merge

#### 风险点

- 逐列 merge 的 `columnWriter.remain` 是累积对象，不能只按 rows 或时间窗口 split。
- `WriteOriginal` 当前使用 `for readSize < meta.size` 复制原 chunk；源 `meta.size` 已回绕时会复制截断数据并生成坏新文件。
- `StreamWriteFile.WriteMeta` 输出 `ChunkMeta.size` 当前会 uint32 窄化，本方案不强制保护该输出。

#### 设计动作

1. `columnWriter.write` / `splitRemain` 改成 rows + bytes 双条件。
2. unordered merge 时间窗口不能只依赖 `maxTime`，还要受 `MaxVarColValBytes` 限制。
3. `WriteOriginal` 不再依赖源 `meta.size`:
   - 按场景使用 `nextChunkMeta.offset - meta.offset` 或 segment entry 覆盖范围计算 `expectedChunkDataSize`。
   - fast-copy 复制范围使用 `[meta.offset, meta.offset + expectedChunkDataSize)`。
   - 分块读取时每次仍可用较小 uint32 buffer size，但循环总长度用 int64。
   - 平移 segment offset 后写新 meta；新 `ChunkMeta.size` 仍维持当前 uint32 输出行为。
4. 如果 segment entry range 本身不可信，禁用 `WriteOriginal`，改走逐 segment rewrite；无法安全重写时返回 corrupt error。

#### 验收点

- 源 `meta.size` 回绕时，`WriteOriginal` 不再只复制前 `meta.size` 字节。
- 源 segment entry range 不可信时，不走 fast-copy。
- 逐列 merge 超低阈值时提前 flush/split。

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

### 7. 诊断与 repair

#### 设计动作

1. 诊断工具扫描:
   - String field offset 非单调或越界。
   - `ChunkMeta.size` 小于 expected size（由 next chunk offset 或 segment entry 计算）。
   - next chunk offset 或 segment entry offset/size 越过文件 data 区。
2. repair 通过低优先级 recompact/rewrite 实现。
3. repair 必须限速、可中断、可回滚。
4. 不默认静默丢行；若必须丢弃，记录审计信息。

### 8. 其他依赖点核查

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

## 六、同版本交付内容

这些修复在一个版本中交付，不再按发布批次拆分。代码一次合入，运行时通过能力开关、路径开关和后台任务类型逐步放量。

### 必须随版本交付

- 新增 `ValidateChunkMetaDataRange`，支持用 next chunk offset 或 segment entry 覆盖范围判断源 `ChunkMeta.size` 是否可信。
- 查询整 chunk 预读失败或校验不通过时，降级到 per-segment read。
- 非流式 compact / merge fastmode 发现源 `ChunkMeta.size` 不可信，或整 chunk decode 后 `ColVal.Offset` 越界时，降级 streamMode。
- `WriteOriginal` 复制长度改为依赖 next chunk offset 或 segment entry 覆盖范围，而不是源 `meta.size`。
- `appendFields` 前做 String field bytes 预算。
- 单值超限拒写。
- 追加会使 memtable `ColVal` 超阈值时返回 `ErrNeedFlush`，触发 snapshot/flush 后重试。
- WAL physical record 长度在 memtable mutation 前检查，超限拆 batch 或拒写。
- 引入 String field 判断、bounded append API 和 rows + var-bytes splitter。
- 高风险 `AppendColVal`、`AppendString`、直接 `uint32(len(cv.Val))` 统一收敛到 bounded API。
- snapshot/flush 按 rows + var-bytes 切分。
- snapshot/flush 内部 offset 推进改为真实写入大小，不依赖回绕 `chunkMeta.size`。
- stream compact 按 bytes 提前 `writeSegment`。
- stream merge 的 `columnWriter` byte-bounded。
- 非流式 compact / merge 大 String field 场景降级到 byte-bounded streamMode。
- compact / merge 输出 `ChunkMeta.size` 维持现状，不作为本方案保护目标。
- 扫描历史 TSSP，识别坏 `ColVal.Offset`、坏 `ChunkMeta.size`、chunk data range 不一致。

### 默认不自动执行

- 历史文件 repair/recompact 不随版本自动运行。
- repair 通过运维开关手动启用，必须限速、可中断、记录审计。

---

## 七、测试策略

### 写入 / WAL / snapshot

- 低 `MaxVarColValBytes` 下，同一 series String field 列接近阈值后继续写入，验证 mutation 前返回 `ErrNeedFlush`。
- 单值超过产品上限，验证不写 memtable、不写 WAL。
- WAL physical record 超安全阈值，验证写 memtable 前拆批或拒写。
- snapshot 只按行数不足以切开的场景，验证 rows + bytes splitter 生效。
- 构造 `ChunkMeta.size` 回绕场景，验证 snapshot writer 内部后续 chunk offset 仍按真实写入大小推进。

### `ChunkMeta.size` 读取降级

- 构造 `ChunkMeta.size` 小于 expected size 的源 meta，分别覆盖 next chunk offset 和 segment entry 两种算法，验证非流式 compact / merge 降级 streamMode。
- 构造 `ChunkMeta.size` 可信但 decode 后 String field `ColVal.Offset` 越界的整 chunk 数据，验证非流式 compact / merge fastmode 降级 streamMode。
- 查询路径触发 `defaultIoSize` 整 chunk 预读但 decode/range check 失败时，验证降级 per-segment read。
- segment entry 本身不可信时，验证查询/compact 返回 corrupt error，不 panic。
- stream compact / stream merge 输出回绕 `ChunkMeta.size` 后，后续 streamMode 仍可按 segment entry 读取。

### Compact / merge

- 多个合法小 segment compact 后总 bytes 超低阈值，验证不会构造超阈值目标 `ColVal`。
- stream compact 的 `continueMerge` 在 bytes 达阈值时提前 `writeSegment`。
- stream merge 的 `columnWriter.remain` 在 bytes 达阈值时提前 flush/split。
- 源 `meta.size` 回绕时，`WriteOriginal` 仍按 next chunk offset 或 segment entry 覆盖范围复制，不生成截断新文件。

### 兼容性

- 旧 TSSP/WAL 新二进制可读。
- 新二进制写出的 TSSP/WAL 仍保持旧格式，旧二进制可读。
- 含坏 offset / 坏 chunk meta 的旧文件，新二进制读不 panic。

---

## 八、改动清单

### 共享 record 能力

- `lib/record/column.go`:String field 判断、checked accessor、bounded append 入口。
- `lib/record/record_check.go`:新增 `ValidateCol` / `ValidateRecord`。
- `lib/record/*`:新增 rows + var-bytes splitter。

### 写入 / WAL

- `engine/mutable/ts_table.go`:memtable append 前预算。
- `engine/shard.go`:处理 `ErrNeedFlush` / `ErrValueTooLarge`，修正 mem size/token 顺序或回滚。
- `engine/wal.go`:WAL physical record 长度预检查，拆 batch / 拒写发生在 memtable mutation 前。

### Snapshot / flush

- `engine/immutable/msbuilder.go`:写 record 前按 rows + var-bytes 切分；`dataOffset` 用真实写入大小推进。
- `engine/immutable/chunkdata_builder_ts.go`:保留 `ChunkMeta.size uint32` 输出现状，不新增 chunk byte cap。
- `engine/immutable/column_builder.go`:保留 segment 写前断言。

### Compact

- `engine/immutable/chunk_iterators.go`:整 chunk 读取前校验源 chunk data range；整 chunk decode 后校验 `ColVal.Offset`；不可信或 offset 越界时触发 streamMode 降级。
- `engine/immutable/stream_compact.go`:`compactColumn` / `continueMerge` 增加 byte 条件；`writeMetaToDisk` 的 `ChunkMeta.size` 输出维持现状。
- `engine/immutable/merge_tool.go` / `merge_self.go`:大 String field、源 `ChunkMeta.size` 不可信，或整 chunk decode 后 `ColVal.Offset` 越界时绕开整 record fast path。

### Merge

- `engine/immutable/merge_performer.go`:`columnWriter` 按 rows + bytes flush/split；`WriteOriginal` 复制长度改用 next chunk offset 或 segment entry 覆盖范围，不依赖源 `meta.size`。
- `engine/immutable/stream_downsample.go`:`StreamWriteFile.WriteMeta` 的 `ChunkMeta.size` 输出维持现状。
- `engine/immutable/unordered_reader.go`:乱序窗口 byte-bounded。

### 查询 / 诊断

- `engine/immutable/tssp_file_meta.go` / `chunk_meta_codec.go`:chunk data range 校验辅助。
- `engine/immutable/tssp_file.go`:整 chunk 预读失败或 size 不可信时降级 per-segment read；`columnData` 前增加边界判断。
- 新增诊断/repair 工具，复用 `Validate*` 和 chunk data range 校验。

---

## 九、灰度与回滚

本版本线格式不变，所有修复随同一版本发布。灰度对象不是代码批次，而是节点/租户/shard、运行时开关、读写路径和后台任务类型。除 repair 外，保护性开关默认打开；如需更保守放量，可在灰度节点上按下列路径启用和观察。

### 灰度开关

| 开关 | 默认值 | 作用 | 回滚方式 |
|------|--------|------|----------|
| `enable_chunkmeta_size_fallback` | 开 | 查询整 chunk 预读失败、size 不可信或 decode 后 offset 越界时，降级 per-segment read | 关闭后回到旧读取路径，但旧路径可能继续受坏 `ChunkMeta.size` 影响 |
| `enable_nonstream_degrade_stream` | 开 | 非流式 compact / merge fastmode 遇到不可信 `ChunkMeta.size` 或解压后 `ColVal.Offset` 越界时，降级 streamMode | 关闭后回到旧 fastmode 行为 |
| `enable_writeoriginal_range_copy` | 开 | `WriteOriginal` 使用 next chunk offset 或 segment entry 覆盖范围复制，不依赖源 `meta.size` | 关闭后回到旧复制长度逻辑 |
| `enable_var_col_budget` | 开 | 写入、snapshot、compact、merge 使用 String field bytes 预算，避免构造超阈值 `ColVal` | 紧急时可关闭，但需要保留告警，避免继续制造坏数据 |
| `enable_wal_precheck` | 开 | WAL physical record 在 memtable mutation 前做长度检查，拆 batch 或拒写 | 关闭后回到旧 WAL 写入逻辑 |
| `enable_background_byte_bound` | 开 | snapshot/flush、stream compact、stream merge 按 rows + bytes 切分或提前 flush | 关闭后后台流程回到旧切分逻辑 |
| `enable_repair_job` | 关 | 允许低优先级 repair/recompact 历史坏文件 | 停止任务即可；已重写文件仍是旧 TSSP 线格式 |

### 放量节奏

- 先打开读侧 fallback 与非流式降级开关，观察查询 per-segment fallback、source chunk meta range 校验失败和 compact/merge streamMode 降级次数。
- 写流量按 shard 或租户逐步放量 `enable_var_col_budget` 与 `enable_wal_precheck`，重点观察 `ErrNeedFlush`、`ErrValueTooLarge`、WAL 拆批/拒写、写入延迟和写失败率。
- 后台任务按类型放量 `enable_background_byte_bound`，先 snapshot/flush，再 stream compact/merge，最后覆盖非流式 compact/merge fastmode 降级路径。
- `enable_writeoriginal_range_copy` 随后台任务一起放量，重点观察 next chunk offset / segment entry range 复制次数、fast-copy 禁用次数和新文件校验结果。
- 诊断工具可默认运行只读扫描；repair/recompact 必须手动开启、限速运行，并支持按文件或目录停止。

### 回滚策略

- 优先回滚具体开关，不做数据格式迁移。
- 如果读侧 fallback 带来不可接受的查询延迟，可关闭 `enable_chunkmeta_size_fallback`，但需要明确旧路径遇到坏 `ChunkMeta.size` 仍可能 decode 失败或读到截断数据。
- 如果写入预算出现误判，可临时关闭 `enable_var_col_budget`，但应保留超限观测指标，并优先修正预算逻辑后重新开启。
- 如果后台任务放量导致资源占用过高，可关闭 `enable_background_byte_bound` 或 `enable_nonstream_degrade_stream`，并限制 compact/merge 并发。
- repair/recompact 可随时停止；已完成 rewrite 的文件不需要反向迁移。

监控:

- `ErrNeedFlush` 次数
- `ErrValueTooLarge` 次数
- WAL 拆批/拒写次数
- source chunk meta range 校验失败次数
- decode 后 `ColVal.Offset` 越界次数
- 查询 per-segment fallback 次数
- 非流式 compact/merge 降级 streamMode 次数
- `WriteOriginal` 使用 next chunk offset / segment entry range 复制次数，及 fast-copy 禁用次数
- compact/merge 提前 write segment 次数
- repair 处理文件数与失败记录数

---

## 十、决策摘要

- 主线保护 `ColVal.Offset`，通过 mutation 前预算、bounded append 和后台流程 byte-bounded，避免构造超阈值 String field `ColVal`。
- `ChunkMeta.size` 不再作为输出侧保护目标。compact / merge 等流程输出维持现状，允许 uint32 回绕继续存在。
- 后续流程必须把源 `ChunkMeta.size` 当作不可信元数据。整 chunk 读、非流式 compact/merge、查询预读、`WriteOriginal` 都需要校验或绕开对它的依赖。
- 非流式 compact / merge 遇到不可信 `ChunkMeta.size`，或整 chunk decode 后 `ColVal.Offset` 越界时降级 streamMode。
- `WriteOriginal` 不能再用 `meta.size` 作为复制长度，应使用 next chunk offset 或 segment entry 推导出的真实覆盖范围。
- 查询路径中 `defaultIoSize` 整 chunk 预读失败时，降级为按 segment 依次读取。
- 即使输出 `ChunkMeta.size` 将错就错，writer 内部 offset 推进仍必须使用真实写入大小，不能依赖已回绕的 `chunkMeta.size`。
