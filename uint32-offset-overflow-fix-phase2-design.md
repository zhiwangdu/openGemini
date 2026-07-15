# TSStore uint32 溢出修复第二步设计:灰度、回滚与运行治理

> 前置设计:[`uint32-offset-overflow-fix-design.md`](uint32-offset-overflow-fix-design.md)
>
> 开发阶段:本文件定义第一步强制安全修复完成后的第二步开发。第一步不实现本文的运行时灰度开关、按租户/节点放量和开关式回滚能力。
>
> 目标:在不改变第一步安全边界和 TSSP 线格式的前提下，为读、写、compact 与 merge 补充可观测、可分批放量和可回退的运行治理能力。

---

## 一、第二步范围与准入条件

第二步只增加运行治理，不重写第一步的数据正确性方案。以下能力必须继续遵守:

- `ColVal.Offset` 仍由 mutation 前预算、bounded append，以及 segment 或 `maxTime + rowsLimit` 有界读取保护；stream compact 保持既有 rows 边界，stream merge 保留既有 merge / `columnWriter` 流程，并限制每批 unordered 最多 1000 行。两者都不依赖 bytes 阈值切分输出 segment。
- `ChunkMeta.size` 线格式保持 uint32；读、compact、merge 继续把源值视为不可信元数据。

进入第二步开发前，第一步至少满足以下条件:

1. 第一步文档“必须随版本交付”的代码与正确性测试全部完成。
2. 低阈值下的 String field 预算、snapshot byte-bounded、非流式 compact/merge 降级和读侧 fallback 均已验证；stream compact 和 stream merge 不要求低阈值提前 split/write，而是分别验证 row-only segment，以及当前 ordered segment 范围和剩余 unordered 的 `rowsLimit` 批读边界。
3. 每个拟新增开关都有明确的作用域、默认值、观测指标、回滚风险和配置一致性要求。

---

## 二、灰度与回滚

第二步仍不改变线格式。灰度对象是节点、租户、shard、读写路径和后台任务类型，而不是重新拆分第一步的数据格式或核心安全不变量。保护性开关默认打开；关闭开关只用于受控回退，并必须明确重新暴露的历史风险。

### 灰度开关

| 开关 | 默认值 | 作用 | 回滚方式与风险 |
|------|--------|------|----------------|
| `enable_chunkmeta_size_fallback` | 开 | 查询整 chunk 预读失败、size 不可信或 decode 后 offset 越界时，降级 per-segment read | 关闭后回到旧读取路径，但可能重新受坏 `ChunkMeta.size`、截断读取或 decode 失败影响 |
| `enable_nonstream_degrade_stream` | 开 | 非流式 compact / merge fastmode 遇到不可信 `ChunkMeta.size` 或解压后 `ColVal.Offset` 越界时，降级 streamMode | 关闭后回到旧 fastmode，可能重新触发超大 record 累积或错误整 chunk 读取 |
| `enable_writeoriginal_range_copy` | 开 | `WriteOriginal` 使用后继 `ChunkMeta` offset（对应下一 series）或 segment entry 覆盖范围复制，不依赖源 `meta.size` | 关闭后回到旧复制长度逻辑，可能复制截断数据 |
| `enable_var_col_budget` | 开 | 写入、snapshot 与非流式 record merge 使用 String field bytes 预算，避免无界累积形成超阈值 `ColVal`；不作用于 stream compact/merge | 仅允许紧急、短时关闭；关闭会重新允许制造坏 offset，必须持续告警并限制流量 |
| `enable_snapshot_byte_bound` | 开 | snapshot/flush 按 rows + bytes 切分 | 关闭后 snapshot/flush 回到旧切分逻辑，可能重新形成超阈值目标 `ColVal`；stream compact/merge 不受该开关影响 |

stream compact 的 row-only segment 边界和 stream merge 的 unordered `rowsLimit` 同样属于第一步正确性不变量，不设置运行时开关。第二步不能关闭 `rowsLimit`，也不能回滚到按当前 segment 时间范围或 `math.MaxInt64` 一次读取全部匹配 unordered 的旧行为。

开关实现还必须满足:

- 开关读取不能位于单次 mutation 的中间；一次 write/compact/merge task 必须使用稳定快照，避免半程切换路径。
- 所有开关必须暴露当前值、作用域和最近变更时间，并记录操作者或控制面来源。
- 禁止用关闭基本边界校验的方式实现“回滚”。

### 放量节奏

1. 先放量读侧 fallback 与非流式降级，观察查询 per-segment fallback、source chunk meta range 校验失败和 compact/merge streamMode 降级次数。
2. 再按 shard 或租户放量 `enable_var_col_budget`，重点观察 `ErrNeedFlush`、`ErrValueTooLarge`、写入延迟和写失败率。
3. 后台任务放量 `enable_snapshot_byte_bound` 时只覆盖 snapshot/flush；stream compact/merge 不参与该开关。随后覆盖非流式 compact/merge fastmode 降级到按 segment 或 `maxTime + rowsLimit` 分批读取的 streamMode 路径。
4. `enable_writeoriginal_range_copy` 随后台任务一起放量，重点观察下一 series 的 `ChunkMeta.offset` / segment entry range 复制次数、fast-copy 禁用次数和新文件校验结果。

### 回滚策略

- 优先回滚具体开关，不做数据格式迁移。
- 读侧 fallback 带来不可接受的查询延迟时，可关闭 `enable_chunkmeta_size_fallback`，但必须接受旧路径仍可能 decode 失败或读到截断数据，并保留告警。
- 写入预算出现误判时，可在限制写流量和持续告警的前提下短时关闭 `enable_var_col_budget`；优先修正预算逻辑后重新开启，不把关闭状态作为长期配置。
- snapshot 资源占用过高时，可关闭 `enable_snapshot_byte_bound`；非流式降级资源开销过高时可关闭 `enable_nonstream_degrade_stream` 并限制 compact/merge 并发。stream merge 的 unordered `rowsLimit` 不可关闭，资源异常时只能限并发或暂停相关 merge task，不能回到单次全量 time-range 读取。

### 监控

- `ErrNeedFlush` 次数。
- `ErrValueTooLarge` 次数。
- source chunk meta range 校验失败次数。
- decode 后 `ColVal.Offset` 越界次数。
- 查询 per-segment fallback 次数。
- 非流式 compact/merge 降级 streamMode 次数。
- `WriteOriginal` 使用下一 series 的 `ChunkMeta.offset` / segment entry range 复制次数及 fast-copy 禁用次数。
- snapshot 因 bytes 阈值提前 split 次数。
- stream merge unordered 批次数、每批行数、`hasMoreWithinRange` 为 true 的次数、单批触达的 source segment/file 数，以及 rowsLimit invariant 失败次数；不监控 bytes 阈值提前 split/flush。

---

## 三、总体决策摘要

- 开发分两步:第一步直接交付强制安全修复，不实现灰度能力；第二步才增加开关、分批放量、运行监控和开关式回滚。
- 主线保护 `ColVal.Offset`，通过 mutation 前预算、bounded append、snapshot byte-bounded，以及 stream segment 或 `maxTime + rowsLimit` 有界读取，避免构造超阈值 String field `ColVal`。
- 写入路径的 P0 聚焦 memtable `ColVal` 整批预算。预算失败发生在任何字段 append 前，通过 rotate/flush/retry 或拒写处理，不实现 mutation 回滚。
- `ChunkMeta.size` 不作为输出侧保护目标。compact/merge 等流程输出维持现状，允许 uint32 回绕继续存在。
- 后续流程必须把源 `ChunkMeta.size` 当作不可信元数据。整 chunk 读、非流式 compact/merge、查询预读、`WriteOriginal` 都需要校验或绕开对它的依赖。
- 非流式 compact/merge 遇到不可信 `ChunkMeta.size`，或整 chunk decode 后 `ColVal.Offset` 越界时降级 streamMode。
- `WriteOriginal` 不能再用 `meta.size` 作为复制长度；当前 `ChunkMeta` 后面存在另一个 series 的 `ChunkMeta` 时使用二者 offset 差值，当前 `ChunkMeta` 位于文件物理顺序末尾时使用 segment entry 推导真实覆盖范围。
- 查询路径中 `defaultIoSize` 整 chunk 预读失败时，降级为按 segment 依次读取。
- stream compact 保持现有 row-only `continueMerge` 和按 1000 行 `writeSegment` 的行为；在 `max-line-size<=1MiB` 的配置前提下，输出 segment 和临时合并对象均低于 uint32 边界，本次及第二步均不为其增加 rows + bytes 条件或提前 `writeSegment` 开关。
- stream merge 不在 `columnWriter` 增加 rows + bytes 条件，也不改变现有 `Handle -> readUnordered -> merge -> columnWriter` 流程。它只给 `ReadTimes` / `Read` 增加 `rowsLimit`：按 ordered 当前 segment 的 `maxOrderTime` 读取，以及最后用 `math.MaxInt64` 排空剩余 unordered 时，每批最多 1000 行并循环处理；该限制属于第一步正确性实现，不设置可关闭的灰度开关。
- 即使输出 `ChunkMeta.size` 将错就错，writer 内部 offset 推进仍必须使用真实写入大小，不能依赖已回绕的 `chunkMeta.size`。
