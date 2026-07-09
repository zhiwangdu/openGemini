# 修复方案讨论记录(与本地 Codex 多轮对话)

> 讨论对象:`uint32-offset-overflow-fix-design.md`。工具:`codex exec` / `codex exec resume --last`(本地 codex-cli 0.142.5,read-only sandbox,仓库内读码)。
> 三轮对话已收敛。本文件记录关键问答与最终结论。

---

## Round 1:Codex 评审原方案

**问题**:请批判性评审「内存升 uint64 + TSSP/WAL 线格式不变 + 分阶段」方案。

**Codex 结论**:主线可成立,但兼容性论证不严,把 WAL/codec 的 4GB 上限简化成了「只有 Offset 是 uint32」。

### A. 兼容性漏洞(已核实)

1. **TSSP `Segment.size` 本身是 uint32**:`tssp_file_meta.go:64,78` `setSize(size uint32)`;写入侧 `uint32(len(b.data)-pos)`(`column_builder.go:292`、`stream_compact.go:1204`)。须保证「编码后的 column segment block」也 < 4GB,不只是 string offset。
2. **WAL 多处 uint32 长度**:`Record.Marshal` 子块大小 uint32(`record_codec.go:34`);`ColVal.Marshal` 的 `Val` 经 `AppendBytes` = `uint32(len(buf))`(`column_codec.go:25`、`binary_encoder.go:218`);WAL physical record header `1+4` 写 `uint32(len(compData))`(`wal.go:52,232`)。W1 不能只守 Offset。
3. **WAL 描述过窄**:line protocol WAL 写原始行(`shard.go:613,621`);`Record.Marshal` 主要影响 ArrowFlight/record 回放(`shard.go:1178`)。需分开论证 line protocol / ArrowFlight / stream / shelf WAL。
4. **`ColVal` 非 TSStore 私有**:executor `ColumnImpl.offset []uint32`(`column.gen.go:120`);chunk codec `AppendUint32Slice(c.offset)`(`chunk_codec.gen.go:238`);shelf WAL `Bytes2Uint32Slice`/`Uint32Slice2byte`(`wal_codec.go:151,239`)。Phase 2 会波及 ColumnStore / executor / shelf。

### B. Phase 顺序

- Phase 0 不要把 `CheckCol/CheckRecord` 全局 panic→error:调用点无 error 路径(如 `compact.go:203`)。应新增 `Validate*() error` 先替换可恢复路径。
- Phase 1 必须「预检 + 原子 flush/retry」,不能在 `AppendString` 里失败:`AppendFieldsToRecord` 逐列就地修改(`record_group.go:52`),中途 error 留半行。
- segment 字节阈值 + 编码断言应提前到 Phase 1/2 交界。
- Phase 3 应为 quarantine/离线修复,不要「跳过坏行继续 compact」(静默丢数据)。

### C. 「编译器驱动迁移」不能保证完整

捕获不了:独立 uint32 offset 类型(executor `ColumnImpl.offset`、`StringValuesWithOffset`、`AppendStringBytes`,`column.gen.go:84,412`)、wire/protobuf(`internal.pb.go:1656` `Column.Offset []uint32`)、codec 长度字段(`AppendBytes`、`Record.Marshal`、WAL header、TSSP `Segment.size`)、unsafe 转换(`Bytes2Uint32Slice`/`Uint32Slice2byte`,`util.go:246`)、生成代码/第三方编码器。需静态扫描规则。

### D. 2GB segment 阈值偏大

`EncodeStringBlock` 先 pack 再编码,内存同时持有 `Val` + length 表 + 编码 buffer → OOM。建议默认 256/512MB,硬上限 1GB;切分按 record 行边界统一算,不是每列独立;单行 string/tag 超阈值必须显式拒绝;stream compact `splitColumn` 现期望只切 2 段会 panic(`stream_compact.go:1037`),应追加前判断、达阈值先 flush。

### E. WAL:倾向 W1,但更严格

保持旧格式,强制每个 WAL physical record、每个 `Record.Marshal` 子块、每个 `ColVal.Val` 和 offset 都 < uint32 上限;超限拆 batch 或拒写;flush 只清 memtable,不能缩小当前 WAL binary。W2 近期不做(会破坏滚动升级/回滚)。

### F. 额外风险/替代

- **`unpackStringV2` 读侧边界漏洞(已核实)**:`offLen` 是元素个数,却只查 `len(src) < offLen`(`encoding.go:503,506,518`),随后按 `i*4` 读 → 越界。Phase 0 必先修。
- `CheckCol` 直接 panic(`record_check.go:98`);读侧加固不能用「坏 offset 当 nil」替代 error(查询静默错)。
- String/Tag 应统一 `isStringLike`:`PadColVal`(`column.go:176`)、`sliceValAndOffset`(`column.go:346`)只判 String 未判 Tag(已核实)。
- **更根本替代**:分段 ColVal / 流式 merge,避免内存构造 4GB+ 连续 `[]byte`。`uint64` 解决回绕,但不解决 4GB+ 连续内存、编码双缓冲、compact OOM。

---

## Round 2:路线 A vs 路线 B

**问题**:在「生产实例 + 历史数据 + 灰度回滚」约束下,A(内存升 uint64)与 B(保持 uint32,用流式/分段 + 预检避免 4GB+ 连续内存)哪条更低风险?

**Codex 结论**:**先走路线 B**。A 可作后续防御增强,但不适合第一阶段主线。

- **A 的 dealbreaker**:`ColVal.Offset` 是公共核心结构(`column.go:30`),改 uint64 制造「双 offset 宽度世界」;线格式仍有硬 uint32(TSSP segment size `tssp_file_meta.go:60,78`、WAL record `wal.go:232`、`AppendBytes` `binary_encoder.go:218`);executor 自维护 `[]uint32`(`column.gen.go:120`);回滚须证明所有新旧节点交界无 uint64 内存态被窄化写入旧格式——证明脆弱。
- **B 的 dealbreaker**:不能允许单 string/tag value 超阈值;必须关闭/改造非流式 merge/compact;切分须从行数扩展为「行数 + 变长列字节」。
- **B 必改点(file:line)**:
  - ingest 预检:`ts_table.go:346,380` → `record.AppendFieldsToRecord`;底层 `column.go:143`、`column_string.go:51,143`。
  - flush:`ts_table.go:93,103,110`;`msbuilder.go:1151,1170`(按行拆,须补字节阈值)。
  - 非流式 compact(高危):`chunk_iterators.go:149` `Merge` + `compact.go:215` `WriteRecord`——整列 merge 到单 record,须禁用或改增量。
  - stream compact:`stream_compact.go:765,1429,1440`(只看 `Len<maxRows`)、`1033`(按 maxRows)——追加前估字节,超阈值先 `writeSegment`。
  - merge:`merge_performer.go:433,446`(`cw.remain` 须按字节 flush);`unordered_reader.go:394,522` + `merge_performer.go:93`(时间窗口须 byte-bounded);`mergeSelfFastMode` `merge_tool.go:217` / `merge_self.go:48,69,72`(须先禁用/改造)。
- **A 的「边界转换」(只改 lib/record,executor 维持 uint32)**:技术可行但不建议作主线——executor `AppendStringBytes` 仍 `uint32(len)`(`column.gen.go:412`);uint64 只把 panic 从 append 点移到编码窄化点;不解决内存问题。
- **先做顺序**:① 修读写边界 fail-closed(`unpackStringV2` + 编码断言);② ingest 预检(256/512MB);③ 所有写出路径加「行数 + 字节数」双阈值;④ 先禁用/绕开 `mergeSelfFastMode` 与非流式 compact 的大 string 路径,统一走流式;⑤ 历史修复最后。

---

## Round 3:收敛最终方案

**Push back**:B 与 A 对称地都有「找齐所有累加点」的完整性风险,且 B 无编译器兜底。为何 B 更可保证?

**Codex 回应**:push back 成立。B 仍推荐,因其可收敛成更强不变量:

> TSStore 写入/flush/compact/merge 中,任何 String/Tag `ColVal.Val` 不得超过 `MaxVarColValBytes`,且所有变长列追加必须经过 bounded append 闸口。

A 的 uint64 只暴露部分 Offset 站点,无法覆盖 `uint32(len(buf))`、WAL 长度、TSSP segment size、protobuf、executor、unsafe、generated code。B 用「集中 append 闸口 + CI 静态扫描 + 编码断言」闭环,要禁止的是少数「构造大 ColVal」入口,比 A 的「证明所有 uint32 边界正确窄化」更可控。

### 集中式闸口(不让 lib/record 直接触发 flush)

- `lib/record` 提供 bounded API:`TryAppendString/Tag`、`TryAppendColVal`、`SplitByRowsAndVarBytes`;超限返回 `ErrNeedFlush` / `ErrValueTooLarge`。
- `engine/mutable`、`MsBuilder`、stream compact、merge writer 收到 `ErrNeedFlush` 后负责 flush/split/retry。
- 旧无错误 append API 只用于 numeric / 小对象 / 测试 / allowlist。
- 静态闸口用自定义 `go vet`/CI analyzer:禁止非 allowlist 写 `cv.Val=append(...)`、`cv.Offset=append(...)`、`uint32(len(x.Val))`、未带 budget 的 `AppendColVal`/`AppendFieldsToRecord`、writer 路径只按行数的 `Split`。

### 分阶段计划(每阶段含不变量 + 静态闸口)

| 阶段 | 不变量 | 关键改动 | 静态闸口 |
|------|--------|----------|----------|
| **Phase 0** 读侧加固 | 解码坏 string/tag block 只返回错误,不 panic | `unpackStringV2` offLen 边界(`encoding.go:503`);`PadColVal`/`sliceValAndOffset` 补 Tag(`column.go:172,336`);`CheckRecord/CheckCol` 完整 offset 单调/边界/长度校验 | decoder fuzz 覆盖 malformed offset;string/tag decoder 禁止直接 panic |
| **Phase 1** 写入口预检 | ingest 后 memtable 内任意 String/Tag `ColVal.Val <= MaxVarColValBytes` | `appendFields` 进 `AppendFieldsToRecord` 前预检(`ts_table.go:346,380`);底层 `column.go:143`/`column_string.go:51`;WAL 写前拆 batch 避免 uint32 超限(`wal.go:232`) | ingest 路径禁止直接调未预检的 `AppendFieldsToRecord` |
| **Phase 2** 集中 bounded append API | 所有 String/Tag 追加先做 byte budget 判断(mutation 前) | 新增 bounded API;迁移 `AppendColVal`/`AppendString`/`appendStringCol`/sort/merge 变长追加;旧 API 保留但 TSStore 写路径禁用 | CI analyzer allowlist:仅 bounded append 文件可写 `Val/Offset` 与 offset 窄化 |
| **Phase 3** 字节阈值切分 + 高危路径处置 | flush/compact/merge 输出任意 String/Tag segment/block < 阈值 | `msbuilder.go:1151,1170`;`column_builder.go:353`;`stream_compact.go:765,1429,1440`;`merge_performer.go:433,446`;先禁用/绕开 `mergeSelfFastMode`(`merge_tool.go:217`)与非流式 compact 整 record merge(`chunk_iterators.go:149`+`compact.go:215`) | writer 路径禁止只按行数的 `Split`,须走 rows+bytes splitter |
| **Phase 4** 历史修复 | repair 后历史 TSSP 满足新 segment byte cap | 先靠 Phase 0 可读不 panic;低优先级 background repair/recompact 重写超阈值 segment;限速、可中断、可回滚 | — |

### 单值超阈值策略

- 明确拒绝写入,不截断/不拆分/不落 WAL;错误发生在 memtable/WAL mutation 前。
- `MaxVarColValBytes` 默认 **256MiB**,可配至 512MiB,不建议更高。
- 返回非重试型客户端错误 `string/tag value too large: size=X limit=Y`。
- batch 无 per-row error 语义时,拒绝整个 batch,避免部分成功。

### A(uint64)是否还需要

**B 落地后不再把 A 作为必做项**。保留 `uint32 Offset`,用 `uint64` 做长度计算与边界判断即可;真正需要的是 fail-closed 编码断言。仅当产品明确要支持「单内存 `ColVal.Val > 4GB`」时 A 才有意义——但这与 B 核心不变量冲突,且带来巨大内存/GC/查询风险。**建议把 A 写成 future non-goal**。

### 必须新增的回归测试

- malformed `unpackStringV2`:`offLen=0`、offset 区长度不足、非单调、超 value 区,均不 panic。
- Tag 路径:`PadColVal`/`sliceValAndOffset`/`Split` 对 Tag 与 String 行为一致。
- ingest 预检:列接近阈值再写小 string/tag 触发 flush/retry,最终无超限 `ColVal`。
- 单值超限:返回客户端错误,memtable/WAL 无副作用。
- WAL batch 拆分:大 batch 拆成多个安全 WAL record。
- snapshot/flush:行数少但 string bytes 超阈值,能按字节拆 segment。
- stream compact:多小 segment 合并后超阈值时提前 `writeSegment`。
- mergeOutOfOrder normal:order+unordered 合并超阈值时 `columnWriter` 分段写。
- `mergeSelfFastMode`:大 string/tag 列不走 fast mode,或 fast mode 已具 byte split。
- 非流式 compact:大 string/tag 历史文件不整列 merge 到单 `ColVal`。
- encoder 断言:超 uint32 或超配置阈值的 string/tag block 写盘前失败。
- CI analyzer golden:TSStore 写路径中直接 `cv.Val=append(...)`、`uint32(len(cv.Val))`、未 bounded 的 `AppendColVal` 必须失败。

### 最大剩余风险

1. **`ColVal.Val`/`Offset` 是 exported 字段,Go 编译器无法禁止绕过闸口** → 缓解:自定义 analyzer + allowlist + 编码断言 + fuzz 四层联用。
2. **历史文件可能合法但超大,repair/compact/query 仍可能内存压力** → 缓解:先禁用高危非流式路径;repair 限速;查询侧加 chunk byte cap;所有大列处理走流式。

---

## 最终判断(收敛)

- **主线改为路线 B**:保持 `uint32` 全域,用「bounded append 闸口 + 字节阈值切分 + 流式 merge/compact + ingest 预检」建立「单 ColVal.Val 不超阈值」不变量。
- **A(uint64 类型变更)降为 future non-goal**,B 落地后不再必做。
- **兼容性**:TSSP/WAL 线格式全不变,历史数据可读、可灰度、可回滚。
- **完整性保证**:不靠编译器,靠「集中 bounded API + CI analyzer + 编码断言 + fuzz」四层闭环。
- **原方案文档需据此修订**:把 Phase 2(内存升 uint64)替换为「bounded append API + 字节阈值切分 + 高危路径处置」;Phase 0 补 `unpackStringV2` 修复与 Tag 一致性;阈值从 2GB 调到 256MiB(可配 512MiB);补 WAL 多处 uint32 长度边界与 batch 拆分。
