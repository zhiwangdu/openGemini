# TSStore TSSP uint32 回绕文件审计工具设计

> 关联文档:
> - `uint32-offset-overflow-fix-design.md`
> - `uint32-offset-overflow-panic-analysis-tsstore.md`
>
> 定位:本工具用于人工检查历史 TSStore TSSP 是否存在 `ChunkMeta.size` 回绕后污染后续 chunk / segment absolute offset 的问题。工具由运维或研发人员手动执行，不集成到查询、compact、merge 或后台定时任务中。

---

## 一、目标与边界

**第一版只做只读审计和报告，不自动修复文件。**

### 1. 目标

- 识别物理布局正常的普通文件和满足修复后 reader 所需不变量的受控回绕文件。
- 识别旧 writer 在列间或 chunk 间使用回绕后的 uint32 size 推进 cursor 后形成的 absolute offset 错位特征。
- 识别单个 `Segment.size` 可能已经回绕、无法从现有元数据唯一还原的文件。
- 输出可归档、可机器解析的逐文件审计结果，供人工决定隔离、下线、恢复或进一步分析。

### 2. 不做

- 不由 `ts-store` 自动触发。
- 不接入查询、compact、merge 或 `WriteOriginal` 的在线调用链。
- 不修改、rename、删除或隔离 TSSP 文件。
- 不生成 corrected-offset sidecar，不让在线 reader 使用推测地址读取历史坏文件。
- 不自动重写、repair 或迁移文件。
- 不实现后台定时扫描、灰度扫描或自动告警闭环。
- 不处理 ColumnStore、detached primary key/data/index 等非 TSStore 文件。

### 3. 与第一步修复的关系

`uint32-offset-overflow-fix-design.md` 负责:

- 修复 writer 的真实 int64 cursor。
- 禁止 `ColVal.Offset` 和 `Segment.size` 在已覆盖路径回绕。
- 允许 `ChunkMeta.size` 受控回绕。
- 使修复后 writer 生成的文件不再因 `cm.size` 回绕污染后续 absolute offset。

本工具负责:

- 对修复版本上线前已经存在的历史 TSSP 做人工核查。
- 在出现疑似错读、decode error、slice 越界或 compact/merge 异常时做定向审计。
- 为是否允许旧文件继续参与查询和后台任务提供人工判断依据。

---

## 二、为什么必须按完整文件审计

旧 `MsBuilder` 在写完一个 chunk 后使用 uint32 `cm.size` 推进下一 SID 的 `dataOffset`。例如真实 chunk 大小为 `2^32 + 100`:

```text
cm1.offset = 16
cm1.size   = 100
cm2.offset = 116
```

此时:

```text
cm2.offset - cm1.offset == cm1.size == 100
```

该差值看起来完全一致，但真实的第二个 chunk 物理起点还应增加 `2^32`。后续 SID 的全部 segment absolute offset 也可能整体少一个或多个 `2^32`。同一 chunk 内，旧 writer 还可能使用窄化后的整列长度推进下一列，导致后续列的 segment offset 出现同类欠推进。

因此以下局部判断都不能独立证明历史文件安全:

- `next.offset - current.offset == cm.size`。
- `entry.offset >= cm.offset`。
- `entry.offset + entry.size` 仍落在文件 data 区。
- 错误地址读取后恰好可以按相同 schema decode。

工具必须读取完整、未按列裁剪的全部 ChunkMeta，并从真实 dataStart 按物理顺序维护独立 cursor。

---

## 三、工具形态

### 1. 命令

新增独立二进制:

```text
ts-tssp-audit
```

建议代码位置:

```text
app/ts-tssp-audit/main.go
app/ts-tssp-audit/audit/
```

不复用 `ts-recover` 命令入口，避免只读审计与备份恢复、覆盖写入等高风险操作混在同一工具中。

### 2. 触发方式

工具只能通过人工命令触发，不注册常驻服务或定时任务。支持以下输入粒度:

- 单个 TSSP 文件。
- measurement 目录。
- shard data 目录。
- 显式文件清单。

目录发现只接收已发布的最终 TSSP 文件，默认跳过 `.init`、compact/merge 临时输出和其他未完成文件；显式传入此类文件时返回参数错误，不进入审计。

建议参数:

```text
--path <file-or-directory>       与 --file-list 二选一，待审计路径
--file-list <path>               与 --path 二选一，逐行指定文件
--recursive                     递归扫描目录
--mode metadata|deep            默认 metadata
--output <report.json>          JSON 报告路径
--concurrency <N>               文件级并发数
--io-rate-limit <bytes/sec>     I/O 限速
--max-segment-decode-bytes <N>  Deep 模式单 segment 完整 decode 上限
--file-timeout <duration>       单文件审计时间上限
--include-order                 扫描 ordered 文件
--include-unordered             扫描 unordered 文件
```

`--path` 和 `--file-list` 必须且只能指定一个，否则返回参数错误，不做扫描。`--mode deep` 始终先执行 metadata 审计。`--include-order` 和 `--include-unordered` 均未指定时默认同时扫描；只指定其中一个时仅扫描对应目录。报告文件不参与输入发现，即使 `--output` 位于扫描目录中也不能被当作 TSSP 扫描。

### 3. 运行前提

TSSP 数据本身不可原地修改，但在线 compact/merge 可能替换或删除文件。第一版推荐在以下环境执行:

1. 停止对应 shard 的 compact/merge，并保持文件集合稳定；或
2. 对数据目录创建一致性快照，在快照副本上执行；或
3. 停止 `ts-store` 后离线执行。

工具使用只读 fd 完成单文件扫描，并在开始和结束时通过 `fstat` 复核 identity。报告至少记录 path、device/inode（平台支持时）、size、mtime/ctime、TSSP logical identity、header/trailer/meta digest；结束时还要确认目录项仍指向同一文件。identity 发生变化，或无法把快照副本稳定映射到线上文件时，结果为 `InconclusiveConcurrentChange`，不能依据该报告放行或隔离线上文件。

---

## 四、审计模式

### 1. Metadata 模式

默认模式，只读取:

- 文件 header 和 trailer。
- MetaIndex。
- 完整、未按查询列裁剪的 ChunkMeta。
- 当前 TSSP 版本对应的固定列 framing 规则；不把任意 4 字节 payload 解释成已经验证的 CRC。

该模式用于快速扫描大量文件，只回答 TSSP 元数据能否唯一描述可信的物理布局，重点发现:

- chunk / segment absolute offset 与重建 cursor 不一致。
- `cm.size` 低 32 位与真实 chunk span 不一致。
- dataStart/dataEnd、MetaIndex、ChunkMeta 数量和结构异常。
- 单 segment size 信息不足以覆盖真实 data 区。

Metadata 模式不读取、decode 全部 payload，因此 `layoutStatus` 通过不等于 payload 已验证；报告中的 `payloadStatus` 固定为 `NotChecked`。

### 2. Deep 模式

仅当 metadata 模式能够唯一重建全部 expected range 时，Deep 模式才进一步:

- 按 expected range 读取每个 segment payload。
- 校验可用的 column CRC；segment 本身没有独立 checksum，可能为零占位的 column CRC 记为 `Unavailable`，不能当作成功或唯一依据。
- 校验 encoding header 和 block 边界。
- 对不超过 encoded / declared-decoded 资源上限的 segment，先 inspect block header，再通过 limit-aware decoder 按列类型执行完整 decode；不在内存中聚合整个 series。
- 校验 time segment 行数、time range 与其他列 segment 行数的一致性。

现有 decoder 需要完整的单 segment `[]byte`，且部分实现会根据 block header 直接扩容输出，因此不能把原始 decoder 直接用于不可信文件。第一版不承诺流式 decode 任意大 segment；超过 `--max-segment-decode-bytes`、declared decoded rows/bytes 或总时间预算时，该 segment 记为 `SkippedResourceLimit`，文件 `payloadStatus` 为 `Partial`，不能写成 `Valid`。工具可使用固定 buffer 增量计算 column CRC，但这不替代 decode。

Deep 模式仍然只读，不输出修正后的 TSSP。`UnrecoverableLayout`、`CorruptMeta`、`Inconclusive*` 等无法唯一确定 range 的文件返回 `DeepNotApplicable`，不尝试从邻近位置搜索可解码 block。

### 3. 资源上限

工具必须对 metadata block bytes、MetaIndex / ChunkMeta / column / segment 数量、单 segment payload、decoded rows/bytes、单文件耗时和总扫描耗时设置上限，并支持 context cancel。所有长度和数量先 checked 再分配；I/O 限速覆盖 metadata 和 deep 的全部读取路径。达到上限时返回 `InconclusiveResourceLimit` 或 `payloadStatus=Partial`，不得误报为 corrupt。

不能直接调用“读出 count 后立即 resize”或“按压缩头声明大小直接解压/扩容”的现有入口。第一版必须提供以下 bounded 能力，且审计工具只能调用这些入口:

```text
ParseChunkMetaHeader
    -> 读取 columnCount / segCount / codec 信息，不分配可变数组
    -> checked 计算内存预算并校验 limits
ParseChunkMetaBodyWithLimits

DecompressChunkMetaWithLimit
    -> 先读取 declared decoded size
    -> 校验上限后再分配和解压

InspectEncodedBlock
    -> 返回 encoding type / declared rows / declared decoded bytes
DecodeBlockWithLimit
    -> 校验 encoded、decoded 和 rows limits 后再分配
```

若某个 TSStore encoding 尚无可安全 inspect 的格式，则对应 segment 只能记为 `SkippedUnsupportedDecoder` / `payloadStatus=Partial`，不得退回原始无界 decoder。metadata 模式同样必须使用 bounded ChunkMeta 解析，不能因为未进入 deep 就跳过资源保护。

---

## 五、文件级审计算法

### 1. 读取完整元数据

1. 校验文件 magic、version 和 trailer。
2. 取得 `dataStart = trailer.dataOffset`。
3. 取得 `dataEnd = trailer.dataOffset + trailer.dataSize`，使用 checked add。
4. 校验 `0 <= dataStart <= dataEnd <= fileSize`，且 data、meta、MetaIndex、trailer 等区间 checked、互不重叠并符合当前版本布局。
5. 按文件编码顺序读取所有 MetaIndex block，校验 block range、count、无重叠和无越界。
6. 通过 bounded header/decompress/body API 完整反序列化所有 ChunkMeta，不使用查询列裁剪接口，也不在 count/decoded-size 校验前分配可变数组。
7. 校验 trailer idCount、MetaIndex count、实际 ChunkMeta 数量、SID 严格递增，以及 MetaIndex 对 ChunkMeta 的覆盖关系。
8. 校验 `columnCount == len(colMeta)`、`segCount == len(timeRange)`、每列 `len(entries) == segCount`，并按版本检查 field/time 列的位置、类型、重复列和空列规则。

后续所谓“物理写入顺序”只能来自文件编码时的 MetaIndex block 顺序和 block 内 ChunkMeta 顺序；不得按 raw `cm.offset` 或 `Segment.offset` 排序，否则历史回绕值可能重排并掩盖错位。

### 2. 重建物理 cursor

当前支持的 TSStore 版本中，每列 payload 前有固定 4 字节 column CRC / 占位 framing。工具按版本选择布局描述符；未知布局直接返回 `UnsupportedFormat`，不得套用 4 字节假设继续扫描。

```text
cursor = dataStart

for cm in encodedMetaOrder:
    reconstructedChunkStart = cursor
    compare cm.offset with reconstructedChunkStart

    for column in cm.storedColumnOrder:
        reconstructedColumnPrefixOffset = cursor
        cursor = checkedAdd(cursor, 4)

        for segment in column.storedSegmentOrder:
            reconstructedSegmentOffset = cursor
            compare segment.offset with reconstructedSegmentOffset
            cursor = checkedAdd(cursor, int64(segment.size))
            require cursor <= dataEnd

    reconstructedChunkEnd = cursor
    reconstructedChunkSize = reconstructedChunkEnd - reconstructedChunkStart
    require uint32(reconstructedChunkSize) == cm.size
```

raw `cm.offset` 和 `Segment.offset` 只用于比较取证，绝不参与独立 cursor 推进。`Segment.size` 是现有元数据唯一提供的 segment 长度；若它本身已经回绕，工具只能报告布局无法唯一恢复，不能把缺失的 `N * 2^32` 猜到某个 segment。

全部 ChunkMeta 处理完成后要求:

```text
cursor == dataEnd
```

若 cursor 小于 dataEnd，工具记录 missing bytes、是否为 `2^32` 的整数倍及可能原因，但不直接断言一定是 segment 回绕；cursor 大于 dataEnd、gap、overlap、低 32 位不一致或其他 offset mismatch 则进入通用布局损坏分类。

### 3. Deep 验证

Deep 模式基于重建出的 expected range 读取数据:

- 读取必须使用 expected range，不使用已经判定错位的 raw offset。
- column CRC 可用固定 buffer 增量计算；单 segment 先通过 `InspectEncodedBlock` 检查声明的 rows/decoded bytes，再由 `DecodeBlockWithLimit` 分配，绝不直接调用无界 decoder 或一次分配整个 chunk。
- column CRC、encoding、decode、行数和 skipped 原因分别写入报告。
- Deep 验证失败不会尝试从邻近位置搜索“可解码”数据，避免把其他 SID 的同 schema block 当作恢复结果。

---

## 六、结果分类

每个文件分别输出 `layoutStatus` 和 `payloadStatus`，避免把 metadata 布局通过误解为 payload 已完整验证。两类状态都附带逐 chunk / column / segment 证据。

### 1. `LayoutValidNormal`

- 所有物理 offset 与独立 cursor 一致。
- 每个 segment size 可表示。
- 每个 reconstructed chunk size 不超过 `MaxUint32`。
- `uint32(reconstructedChunkSize) == cm.size`。
- 最终 cursor 等于 dataEnd。

### 2. `LayoutValidControlledWrap`

- 所有物理 offset 与独立 cursor 一致。
- 每个 segment size 可表示。
- 至少一个 reconstructed chunk size 超过 `MaxUint32`。
- `cm.size` 等于真实大小低 32 位。
- 最终 cursor 等于 dataEnd。

该状态只表示文件布局满足修复后 reader 所需的受控回绕不变量，不证明生成该文件的 writer 版本，也不表示旧二进制可读。例如旧 writer 的最后一个 chunk 也可能恰好满足该布局。

### 3. `LegacyOffsetPattern`

- raw ChunkMeta / Segment absolute offset 与独立 cursor 不一致；且
- 欠推进量符合此前整列或整 chunk 真实长度高 32 位丢失的累计特征。

该状态是高置信度原因模式，不是文件格式记录的 writer provenance。raw offset 对在线链路仍不可信；第一版只报告，不生成 corrected offset map 给在线服务使用。

### 4. `CorruptLayout`

元数据可以解析，但布局不满足有效或 legacy wrap pattern，包括:

- `uint32(reconstructedChunkSize) != cm.size`。
- gap、overlap、非 `N * 2^32` 特征的 offset mismatch。
- cursor 超过 dataEnd、raw entry 顺序异常或无法解释的 trailing data。
- count、column/time 布局等结构关系不成立，但尚未达到无法反序列化的程度。

诊断必须给出 `reason`，例如 `ChunkSizeLow32Mismatch`、`Gap`、`Overlap`、`OffsetMismatch` 或 `TrailingData`。

### 5. `UnrecoverableLayout`

- 元数据描述的 cursor 无法到达 dataEnd；且
- 可能存在单个 segment 真实长度超过 `MaxUint32`，`Segment.size` 已丢失整倍数信息；或
- 多个候选布局均能解释低 32 位字段，无法唯一确定物理边界。

该类文件不能通过猜测倍数恢复。报告使用 `suspectedCause`、`confidence`、`missingBytes` 和 `multipleOf2To32` 描述证据；只有首次缺口能定位到具体 segment 时，才把 `SegmentSizeWrap` 作为高置信度原因。

### 6. 其他 layout 状态

- `CorruptMeta`:header、trailer、MetaIndex 或 ChunkMeta 无法完整解析。
- `InconclusiveConcurrentChange`:扫描期间文件 identity 变化。
- `InconclusiveIO`:权限、短读或 I/O 错误导致扫描未完成。
- `InconclusiveResourceLimit`:达到资源或时间上限，metadata 审计未完成。
- `UnsupportedFormat`:不是本工具支持的 TSStore TSSP 版本。

`Inconclusive*` 不能被当作有效文件结论。

同一文件命中多个 layout 异常时，`layoutStatus` 按以下优先级选择（左高右低）：`Unsupported/Inconclusive -> CorruptMeta -> UnrecoverableLayout -> CorruptLayout -> LegacyOffsetPattern -> LayoutValid*`。全部次级证据仍保留在 diagnostics 中。

### 7. Payload 状态

- `NotChecked`:metadata 模式，未执行 deep。
- `Valid`:deep 对全部 segment 完成允许的 framing、decode 和语义校验；不可用的零占位 column CRC 单独记录，不伪造 checksum 成功。
- `Corrupt`:唯一 expected range 上的 encoding、decode、行数或有效 column CRC 校验失败。
- `Partial`:至少一个 segment 因资源上限或缺少安全 inspect/decode 能力记为 `SkippedResourceLimit` / `SkippedUnsupportedDecoder`，其余结果照常报告。
- `DeepNotApplicable`:layout 无法唯一重建，不能安全执行 deep。
- `Inconclusive`:deep 因 I/O、并发变化或取消未完成。

---

## 七、报告与退出码

### 1. JSON 报告

报告至少包含:

```text
toolVersion
scanStart / scanEnd
arguments
fileIdentity
scanMode
layoutStatus
payloadStatus
dataStart / dataEnd
chunkCount
wrappedChunkCount
firstMismatch
diagnostics[]
recommendedAction
```

`firstMismatch` 包含 SID、column、segment、raw offset/size、reconstructed offset、差值、`sizeStatus` 和可选 reconstructed size。metadata 无法唯一得到真实 segment size 时，`reconstructedSize` 必须为 `null`，不得把 raw uint32 size 伪装成真实值。

所有 int64 offset/size 在 JSON 中使用十进制字符串，避免 IEEE-754 消费端丢失精度。diagnostics 设置数量上限，只保留总计数、首个和最后若干证据；默认不输出 field value 或原始 payload，避免报告携带用户数据或在大面积错位时无限膨胀。

### 2. 终端摘要

```text
scanned=1000
layout_valid_normal=998
layout_valid_controlled_wrap=1
legacy_offset_pattern=1
corrupt_layout=0
unrecoverable_layout=0
payload_partial=0
inconclusive=0
```

### 3. 退出码

- `0`:metadata 模式下全部文件为 `LayoutValidNormal` / `LayoutValidControlledWrap`；或 deep 模式下这些文件同时为 `payloadStatus=Valid`。
- `1`:工具内部错误或参数错误。
- `2`:发现 `LegacyOffsetPattern`、corrupt、unrecoverable 或 `payloadStatus=Corrupt`。
- `3`:存在 unsupported、`Inconclusive*`、`payloadStatus=Partial/Inconclusive`，审计未形成完整结论。

当同时存在 corrupt 和 inconclusive 时返回 `2`，报告中仍分别计数。

---

## 八、人工处理流程

人工工具不是在线代码依赖，但若要把“历史错位文件不会进入修复后查询、compact 或 merge”作为发布保证，完整审计和处置必须成为运维准入条件；未覆盖全部既有文件就上线，等同于显式接受未审计文件仍可能静默错读或错误复制的残余风险。

1. 固定待扫描文件集合，优先使用一致性快照。
2. 对目标目录执行 metadata 审计并保存 JSON 报告。
3. 只有能唯一重建 expected range 的 `LayoutValid*`，以及范围唯一的 `LegacyOffsetPattern`，才可按需要执行 deep；`CorruptMeta` / `UnrecoverableLayout` 不做邻近探测。
4. `Inconclusive*` 先排除并发替换、I/O 或资源问题，再重新执行 metadata，不得按有效结果放行。
5. 人工按稳定 file identity 复核首个错位位置和影响范围，确认报告对应的线上文件没有被替换。
6. `LayoutValidNormal` / `LayoutValidControlledWrap` 只可作为 offset/wrap 布局通过的依据；若操作还要求 payload 完整性，必须同时满足 `payloadStatus=Valid`。
7. `LegacyOffsetPattern` / `CorruptLayout` / `UnrecoverableLayout` 按 identity 从正常流量中人工隔离，后续恢复另立方案。

完整发布准入流程为：停止目标 shard 的旧 writer 并冻结 compact/merge，审计全部既有最终 TSSP，按稳定 identity 隔离 invalid 文件，再允许修复后版本处理其余文件。`Inconclusive*` 必须先重跑至形成确定结论；仍无法消除时保持对应 shard / 文件集合离线或不放行，不能依据 inconclusive 报告执行文件级隔离。工具本身不执行文件移动、服务配置变更或版本发布。

---

## 九、测试策略

### Metadata 审计

- 普通多 SID 文件审计为 `LayoutValidNormal`，且 metadata 模式的 `payloadStatus=NotChecked`。
- 使用 fake reader、虚拟 DataSize 或稀疏文件构造 `reconstructedChunkSize = 2^32 + 100` 且后续真实 offset 正确的文件，审计为 `LayoutValidControlledWrap`，不在 CI 写入真实 4GiB payload。
- 覆盖最后一个 chunk 回绕、连续多个 chunk 回绕、真实大小跨多个 `2^32`，以及整列回绕导致同 chunk 后续列错位的场景。
- 构造旧 writer 使用回绕 size 推进下一 SID 的文件，`next.offset-current.offset == cm.size`，仍能识别为 `LegacyOffsetPattern`。
- 构造错误 segment offset 恰好命中前一 SID 同 schema block，仍报告错位，不因可 decode 而放行。
- 构造单 segment size 丢失 `2^32` 倍数信息且无法唯一重建的文件，审计为 `UnrecoverableLayout` 并输出置信度，不武断填充 reconstructed size。
- 覆盖低 32 位不一致、非 `N * 2^32` offset mismatch、gap、overlap、cursor 小于/大于 dataEnd、trailing data，验证报告 `CorruptLayout` 及具体 reason。
- header、trailer、MetaIndex、ChunkMeta 截断分别报告 `CorruptMeta`。
- 覆盖多 MetaIndex block、越界/重叠区间、重复或逆序 SID、column/time count 与位置异常，并验证工具始终按编码顺序而不是 raw offset 排序。
- 构造极小 metadata/compressed 输入却声明超大 columnCount、segCount 或 decoded size，验证在分配前返回 `InconclusiveResourceLimit`。

### Deep 审计

- column CRC 有效、无效和零占位三种情况；零占位记为 `Unavailable`，不声称 segment checksum。
- encoding header、行数、time range 和 schema type 不一致。
- 大文件使用受限 buffer，验证不会一次加载整个 chunk；超过单 segment decode 上限时记为 `SkippedResourceLimit` / `payloadStatus=Partial`。
- 构造极小 encoded block 却声明数 GiB decoded output / rows，验证 `InspectEncodedBlock` 在 decoder 扩容前拒绝；不支持安全 inspect 的 encoding 记为 `SkippedUnsupportedDecoder`。
- deep decode 失败后不搜索邻近可解码 block。
- `CorruptMeta`、`UnrecoverableLayout` 和 `Inconclusive*` 返回 `DeepNotApplicable`。

### 工具行为

- 文件扫描期间 size/mtime/inode 变化，或发生 rename/unlink 后同路径重建同 size 文件，结果为 `InconclusiveConcurrentChange`。
- 权限、短读和文件删除产生 `InconclusiveIO`。
- 并发数、I/O 限速、metadata/count/decode 上限和取消生效；资源上限不会被误报为 corrupt。
- 工具运行前后所有目标 TSSP 的内容和名称完全不变；只允许新增显式指定的报告文件。
- JSON 报告不包含 field value 或 payload。
- JSON int64 使用十进制字符串，ambiguous size 为 null，diagnostics 数量受限。
- 单文件、目录、递归和 file-list 输入结果一致。
- 目录扫描跳过 `.init`、compact/merge 临时输出和其他未完成文件；显式输入时拒绝执行。

---

## 十、改动清单

- `app/ts-tssp-audit/main.go`:命令行入口、参数校验和退出码。
- `app/ts-tssp-audit/audit/scanner.go`:文件发现、identity 复核、并发和限速。
- `app/ts-tssp-audit/audit/metadata.go`:完整 MetaIndex / ChunkMeta 读取和物理 cursor 重建。
- `app/ts-tssp-audit/audit/deep.go`:增量 column CRC、资源受限的整 segment encoding/decode 验证和 skipped 状态。
- `app/ts-tssp-audit/audit/report.go`:JSON 报告和终端摘要。
- `engine/immutable/tssp_file_meta.go` / `chunk_meta_codec.go`:提供 header/body 分阶段、count-aware 的 bounded ChunkMeta 解析；compressed meta 在读取 declared decoded size 并校验上限后才分配。
- `lib/encoding`:为 TSStore int/float/bool/string/time block 提供无大分配的 `InspectEncodedBlock` 和 limit-aware decode；尚未覆盖的 encoding 不允许回退到无界 decoder。
- 在线引擎只共享上述无副作用的 bounded 解析原语；工具调度、扫描状态和报告逻辑不得接入查询、compact 或 merge。

---

## 十一、后续扩展

以下能力需要单独评审，不属于第一版工具:

- corrected-offset sidecar。
- 离线 TSSP 重写与校验。
- 自动 quarantine。
- 与告警、工单或发布平台集成。
- 在线服务读取审计报告并执行访问控制。
