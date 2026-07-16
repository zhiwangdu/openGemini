# TSStore Attached TSSP Large Chunk 设计

> 文档状态：方案设计，代码尚未实施。
>
> 前置方案：[ColVal uint64 基础迁移与 32 位边界适配设计](./colval-uint64-foundation-design.md)。
>
> 本文只负责 ChunkMeta.size、TSSP reader/writer 以及直接消费 chunk 的路径。ColVal.Offset 全仓迁移、WAL replay 分批优化、merge streamMode 工作集优化均由独立工作项负责。

---

## 一、结论

本方案采用单字段模型：

~~~go
type ChunkMeta struct {
    sid         uint64
    offset      int64
    size        uint64 // 内存中始终表示 chunk data 的真实长度
    columnCount uint32
    segCount    uint32
    timeRange   []SegmentRange
    colMeta     []ColumnMeta
}
~~~

核心决策如下：

1. ChunkMeta.size 从 uint32 直接升级为 uint64，不再引入 sizeLow、actualSize、known、scope 等并行状态。
2. 对格式有效、且通过所属 layout policy 校验的文件，任何成功返回给调用方的 ChunkMeta，size 都必须是真实物理长度。禁止部分对象保存磁盘低 32 位、部分对象保存真实值。
3. TSSP 磁盘格式不变。marshal 时继续写 uint32(cm.size)；该字段逐步降级为兼容字段和低 32 位校验值。
4. TSStore attached 文件在 unmarshal 阶段扫描完整 ColumnMeta/Segment 元数据，计算真实 chunk 范围，并覆盖磁盘解出的 size。
5. 选择性反序列化可以只物化查询需要的列，但不能只扫描选中列；被跳过列的 Segment 元数据仍要参与真实范围计算。
6. ColumnStore、detached/OBS 等尚未开放 large chunk 的布局继续执行 LegacyChunk32。其磁盘 size 无损扩宽为 uint64，同时 writer 强制 size 不超过 MaxUint32。
7. writer 的物理游标使用 writer.DataSize()，列累计长度、chunk 累计长度和复制剩余长度统一使用 uint64/int64；Segment.size 继续保持 uint32。
8. query 使用恢复后的真实 size 判断是否整块预读。large chunk 不整块预读，仍按 Segment.offset 和 Segment.size 读取。
9. whole-chunk consumer 不允许通过 uint32(cm.size) 继续工作。能够分块或按 segment 处理的路径使用真实 size；不能处理的路径在读取和产生输出前显式返回能力错误。只有目标 stream 路径已被独立证明安全时才能返回 ErrRequireStream。
10. 只有磁盘 size 回绕、绝对 Segment.offset 正确的历史文件可以由新 reader 正常恢复。绝对 offset 已污染的文件不在自动修复范围，必须告警、审计和隔离。

这套模型的主要收益是：cm.size 在内存中只有一种含义，过滤 ColumnMeta、查询选择列或 clone 后不需要维护 actualSizeKnown 一类状态；磁盘兼容逻辑集中在 codec 边界。

---

## 二、背景与问题

### 2.1 当前风险

当前 ChunkMeta.size 是 uint32，并同时承担三种职责：

- writer 编码时累计 chunk 长度；
- reader 判断 chunk 的物理范围；
- query、compact、merge 等路径决定整块读取长度。

当单个 SID 的 encoded chunk 超过 MaxUint32 时，size 会回绕。现有代码中的主要风险包括：

1. MsBuilder 使用 cm.size 推进下一 SID 的 dataOffset，可能把后续 ChunkMeta 和 Segment.offset 写到错误位置。
2. ChunkDataBuilder 会先把整列累计编码长度转为 uint32，再推进下一列 offset。即使每个 Segment 都小于 4 GiB，整列累计超过 4 GiB 后仍会污染后续列 offset。
3. query 可能按回绕后的 size 读取过短窗口，随后在 columnData 中进行未检查切片并 panic。
4. ChunkIterator、fast/nonstream compact、merge、export 等整块读取路径可能少读、截断或错误地把异常当成 EOF。
5. WriteOriginal 的复制循环使用 uint32 计数，large chunk 会提前结束并丢数据。

当前 MsBuilder 在编码后直接写文件，并不会按刚生成的 cm.size 回读数据。因此“线上没有出现相关 panic”不能证明历史文件一定没有被写坏。

### 2.2 为什么不使用 shard 级 ErrValueTooLarge

目标业务是大 String、多 series、短时间 GB 级写入。用 shard 级 memtable 上限保护极端的“单 series、单列超过 4 GiB”会让正常的多 series 大流量写入频繁轮转或背压，代价不可接受。

本方案将数值正确性与资源保护拆开：

- ColVal.Offset 和 ChunkMeta.size 通过 64 位内存类型消除回绕。
- Segment 和磁盘字段继续保持原格式。
- node/shard 的内存预算仍可做资源背压，但不能再把 uint32 格式上限等价为 shard resident 上限。

### 2.3 Segment 边界

Segment.size 继续是 uint32。当前每 1000 点一个 Segment，在现有配置和单值上限下不会达到 4 GiB；实现仍必须在最终编码长度处 checked narrow，防止未来配置变化或异常输入破坏该前提。

large chunk 的合法形态是：

~~~text
每个 Segment.size <= MaxUint32
多个合法 Segment / 多列累计后的 ChunkMeta.size > MaxUint32
~~~

本方案不支持单个 encoded Segment 超过 MaxUint32。

---

## 三、范围与非目标

### 3.1 本文覆盖

- 全局 ChunkMeta.size 的 uint32 到 uint64 类型升级。
- fixed 和 self-compressed ChunkMeta codec 的兼容读写。
- TSStore attached 的真实 chunk 范围恢复。
- full、selected-column 和 sequence iterator 等所有 ChunkMeta 解码入口。
- ChunkDataBuilder、MsBuilder 和 stream writer 的长度累计与物理游标。
- buffered/segment-streaming snapshot 路由和 encoded 工作集上界。
- query preload、Segment direct read 和 checked window。
- ChunkIterator、compact、merge、WriteOriginal、export 等直接消费 cm.size 的路径。
- 历史 size 回绕的恢复、异常分类和告警。
- reader-first 发布、回滚边界和测试。

### 3.2 非目标

- 不修改 TSSP version、ChunkMeta 字段顺序或字段宽度。
- 不修改 Segment.size 的磁盘类型。
- 不重新设计 ColVal.Offset、packString 或 unpackString；由前置文档负责。
- 不修复已经污染的绝对 ChunkMeta.offset 或 Segment.offset。
- 不在本文实现 WAL replay 的分批 checkpoint/中途 flush。
- 不在本文实现 merge streamMode 的工作集上限优化。
- 不在本文开放 ColumnStore、detached/OBS、PK、fragment 或 skip-index 的 large-chunk writer。
- 不以本文为由扩大单个 String value、Record wire 或其他 legacy ABI 的 32 位边界。

WAL replay 和 merge streamMode 虽然不在本文修改，但在对应独立能力完成前，相关场景不得被误标为“已完整支持 large chunk”。

---

## 四、统一内存语义

### 4.1 唯一不变量

~~~text
对任何成功构造，或从格式有效且受支持的文件中解码完成、已经暴露给调用方的 ChunkMeta：

    cm.offset 是 chunk data 的物理起点
    cm.size   是 chunk data 的真实 uint64 字节长度

    checkedEnd = cm.offset + cm.size
    checkedEnd 不发生 int64/uint64 溢出
~~~

磁盘中读出的 uint32 只允许存在于 codec 的局部变量 wireSizeLow 中，不能先写入 cm.size 后把半初始化对象暴露给调用方。

### 4.2 共享结构与解码策略

ChunkMeta 是 TSStore、ColumnStore 和 detached 共用结构。为保证单字段语义，所有生产解码入口必须显式选择策略：

~~~go
type ChunkSizePolicy uint8

const (
    ChunkSizeTSAttached ChunkSizePolicy = iota
    ChunkSizeLegacy32
)

type ChunkMetaDecodeContext struct {
    Policy    ChunkSizePolicy
    DataStart int64
    DataEnd   int64

    // 顺序 iterator 已持有下一项时用于增强校验；随机查询可不提供。
    NextChunkOffset int64
    HasNext         bool
}
~~~

- ChunkSizeTSAttached：扫描完整的列/Segment 元数据，恢复真实 size，并校验 wireSizeLow。
- ChunkSizeLegacy32：将 wireSizeLow 无损扩宽为 uint64；对应 writer 必须保证从未生成 large chunk。

禁止 generic unmarshal 在不知道 policy 时自行猜测。reader、iterator 和测试 fixture 都必须明确传入策略。TS attached 解码必须同时取得当前文件的 trailer data range；NextChunkOffset 只作为顺序读取时的额外证据，不写入 ChunkMeta，也不是计算真实 size 的前提。

policy 不能由每次 ChunkMeta 调用临时指定，否则 attached 文件误传 Legacy32 会重新把 low32 当成真实长度。它必须在打开文件时确定并固化：

~~~text
MmsTables / ImmTable engine type
        -> OpenTSSPFile / NewTSSPFileReader
        -> tsspFile.layoutPolicy
        -> tsspFileReader.decodeContext
        -> 所有 ChunkMeta decoder
~~~

具体要求：

- TSStore attached shard 固化为 ChunkSizeTSAttached。
- ColumnStore、detached/OBS 固化为 ChunkSizeLegacy32。
- DetachedChunkMetaReader、segment sequence reader 等不经过 UnmarshalChunkMetaAdaptive 的入口也必须从 file reader 取得同一 policy。
- TS Parquet、CS Parquet、备份、审计等按路径直接打开文件的工具必须显式提供 engine/layout；缺失时打开失败，不能默认 Legacy32。
- policy 绑定到 file reader 后不可由 query、iterator 或 task 覆盖。
- 不能通过文件名、是否 ordered 或当前选择了哪些列推断 layout。

如果未来 ColumnStore 或 detached 支持 large chunk，应增加各自的范围恢复算法，而不是复用 TS attached 的 CRC/列布局假设。

### 4.3 与 ChunkMeta.Size() 的区别

当前 ChunkMeta.Size() 返回的是 ChunkMeta 元数据条目本身的编码字节数，不是 chunk data 长度。该语义必须保持，否则 meta block sizing 会被破坏。

建议增加语义明确的访问器：

~~~go
func (m *ChunkMeta) DataSize() uint64
func (m *ChunkMeta) DataRange() (start int64, end int64, err error)
~~~

可以后续把现有 Size() 重命名为 EncodedMetaSize()，但不应与本次类型迁移混在同一批机械修改中。

### 4.4 clone、filter 和 reset

- Clone 必须复制真实 size。
- FilterColMeta、DelEmptyColMeta 和查询列过滤不改变 size；size 已经是独立恢复出的物理事实，不再依赖当前保留了哪些列。
- relocation 后，目标 ChunkMeta.size 仍等于源数据长度，但 offset 和所有 Segment.offset 必须按同一 delta 更新。
- reset 把 size 置零。
- writer 修改实际 chunk 字节后必须重新计算 size，不能沿用 clone 的旧值。

---

## 五、磁盘兼容与字段废弃

### 5.1 线格式不变

fixed ChunkMeta 继续使用：

~~~text
sid:uint64
offset:int64
size:uint32
columnCount:uint32
segCount:uint32
...
~~~

self-compressed ChunkMeta 的 size 仍编码为 uint32 语义的 uvarint。

普通 chunk：

~~~text
cm.size <= MaxUint32
wireSizeLow == uint32(cm.size)
~~~

large TS attached chunk：

~~~text
cm.size > MaxUint32
wireSizeLow == low32(cm.size)
~~~

### 5.2 marshal

两套 codec 都必须显式截取低 32 位：

~~~go
wireSizeLow := uint32(cm.size)

// fixed
dst = numberenc.MarshalUint32Append(dst, wireSizeLow)

// self-compressed
dst = binary.AppendUvarint(dst, uint64(wireSizeLow))
~~~

不能让 self-compressed codec 直接编码 uint64 cm.size，否则两套格式会出现不同语义。

受控 low32 只能出现在统一的 checked marshal 入口。以下调用旁路必须收敛：

- MarshalChunkMeta；
- fixed cm.marshal；
- detached 直接调用 cm.marshal 的路径；
- stream compact/downsample 的 metadata 输出。

建议统一为：

~~~go
func MarshalChunkMetaChecked(
    ctx *ChunkMetaCodecCtx,
    cm *ChunkMeta,
    policy ChunkSizePolicy,
    dst []byte,
) ([]byte, error)
~~~

校验规则：

- TS attached 允许 cm.size 大于 MaxUint32，写 low32。
- Legacy32 要求 cm.size 不大于 MaxUint32，否则返回 ErrLargeChunkUnsupported。
- offset + size 必须 checked。
- columnCount、segCount 和实际 metadata 结构必须自洽。

### 5.3 unmarshal

fixed codec：

~~~go
wireSizeLow := numberenc.UnmarshalUint32(src)
~~~

self-compressed codec：

~~~go
n, ok := dec.Uvarint()
if !ok || n > math.MaxUint32 {
    return ErrCorruptTSSP
}
wireSizeLow := uint32(n)
~~~

然后根据 policy 恢复并设置 cm.size。self-compressed 输入大于 MaxUint32 不是合法的新格式，而是畸形 metadata，不能直接截断。

### 5.4 逐步废弃

不升级 TSSP version 时，文件中的四字节字段不能物理删除，只能逻辑废弃：

1. 新 reader 不再把它作为 TS attached 的真实读取长度。
2. 它用于校验 uint32(cm.size) 是否一致。
3. 新 writer 继续写入，保证普通文件与旧版本 byte-for-byte 兼容。
4. 未来只有在新 TSSP version 中才能真正删除或重新定义该字段。

---

## 六、TS attached 真实 size 恢复

### 6.1 物理布局

TS attached chunk 的数据布局是：

~~~text
[4-byte column CRC][column Segment 0][column Segment 1]...
[4-byte column CRC][next column segments...]
...
[4-byte time CRC][time segments...]
~~~

需要注意：

- 第一个 Segment.offset 不是 chunk 起点，前面还有 4 字节 CRC。
- ColumnMeta 可能在写完数据后按列名重排，数组顺序不等于物理顺序。
- selected-column decode 可能只物化少量列。

因此不能直接使用 colMeta[0] 和 colMeta[len-1] 计算。

### 6.2 统一扫描算法

unmarshal 在解析 wire metadata 时，对所有列执行轻量扫描。即使某列不需要物化，也要解析足以计算物理区间的 Segment offset/size。

~~~text
start = cm.offset
intervals = []

for each wire ColumnMeta:
    require entry count == segCount
    require entry count > 0

    first = first Segment
    colStart = checkedSub(first.offset, 4)
    expected = first.offset

    for each Segment:
        require Segment.offset == expected
        segEnd = checkedAdd(Segment.offset, Segment.size)
        expected = segEnd

    intervals.append([colStart, expected])

sort intervals by colStart

expected = start
for interval in intervals:
    require interval.start == expected
    require interval.end > interval.start
    expected = interval.end

actualEnd = expected
require dataStart <= start < actualEnd <= dataEnd
if next chunk offset is available:
    require actualEnd == next chunk offset
actualSize = uint64(actualEnd - start)
require uint32(actualSize) == wireSizeLow

cm.size = actualSize
~~~

该算法验证：

- 每列 CRC 前缀恰好为 4 字节；
- 同列 Segment 连续；
- 各列物理区间无洞、无重叠；
- 元数据顺序变化不影响结果；
- 计算过程不发生 signed/unsigned 溢出；
- 恢复出的真实长度与磁盘低 32 位一致。

对于顺序 iterator，如果已经取得下一 ChunkMeta.offset，还应校验 actualEnd 等于 next.offset；最后一个 chunk 校验 actualEnd 等于 trailer data end。随机 query 没有相邻 metadata 时，这项属于增强校验，不要求为了恢复 size 引入跨 meta-block lookahead。

### 6.3 selected-column decoder

现有 fixed 和 self-compressed selected decoder 都只保留命中的列，并重写 columnCount。新实现应区分：

- wireColumnCount：驱动完整 metadata 扫描和真实范围计算；
- selected ColumnMeta：只保存调用方需要的数据；
- cm.columnCount：保持现有 selected 结果语义，避免扩大查询内存。

推荐在同一次扫描中完成：

1. 解码 ChunkMeta base attributes，把磁盘 size 保存到局部 wireSizeLow。
2. 解析所有列的区间信息。
3. 对选中列完整物化 ColumnMeta；对未选列只解析/跳过并更新区间。
4. 完成物理连续性和 low32 校验。
5. 设置 cm.size。
6. 成功后才把 ChunkMeta 返回给调用方。

第一版也可以先完整 unmarshal，再过滤列，但必须做 metadata CPU/RSS benchmark。不能在已经过滤完成后仅依靠剩余列推导 size。

sequence iterator 当前可能只请求 value 列，也必须走上述扫描，不能假设 time ColumnMeta 一定被物化。

### 6.4 Legacy32 解码

对于 ColumnStore、detached 等 Legacy32 policy：

~~~go
cm.size = uint64(wireSizeLow)
~~~

这是基于 Legacy32 格式契约的无损扩宽，不是回绕恢复。该 policy 的前提是对应 layout 的合法文件从未产生大于 MaxUint32 的 chunk；若历史或畸形文件违反此前提，它属于 unsupported/corrupt input，不在“成功解码后 size 为 actual”的承诺内。

Legacy32 reader 仍要执行所属 layout 已有的 data range 校验；R1 inventory 对来源未知的存量文件确认其满足 Legacy32。不能仅因 uint64(wireSizeLow) 转换本身成功就把文件标记为 verified。

以下独立入口必须显式纳入，不能只修改 UnmarshalChunkMetaAdaptive：

- DetachedChunkMetaReader；
- tssp_file_detached_reader；
- segment_sequence_reader；
- detached MsBuilder 直接调用 fixed marshal 的路径。

这些入口统一绑定 ChunkSizeLegacy32，并在 writer 侧强制 cm.size 不超过 MaxUint32。

---

## 七、writer 改造

### 7.1 长度类型

类型规则如下：

| 对象 | 类型 | 说明 |
|---|---:|---|
| Segment.size | uint32 | 单 Segment 线格式不变，最终编码后 checked narrow |
| Column encoded total | uint64/int64 | 可能由多个 Segment 累计超过 MaxUint32 |
| ChunkMeta.size | uint64 | 内存真实 chunk 长度 |
| ChunkMeta.offset / Segment.offset | int64 | 文件绝对位置，所有加法 checked |
| writer.DataSize() | int64 | 物理写入游标 |
| copy remaining | uint64 | large range 分块复制 |

禁止先把列累计长度转成 uint32，再转回 int64 推进 offset。

### 7.2 ChunkDataBuilder

需要修改的关键点：

- chunkMeta.size 的所有累计自然升级为 uint64。
- 整列 encoded delta 从 len(buffer) 的差值直接生成 uint64/int64。
- 下一列 offset 使用未截断的列累计长度。
- EncodeTime 中每个 Segment 的最终长度 checked narrow 到 uint32，然后以 int64 推进 offset、以 uint64 累计 chunk size。
- CRC 大小参与 uint64 累计。

示意：

~~~go
columnBytes := uint64(len(chunk) - columnStart)
nextOffset, err := checkedAddInt64Uint64(columnOffset, columnBytes)
if err != nil {
    return err
}
~~~

错误写法：

~~~go
columnBytes := uint32(len(chunk) - columnStart)
nextOffset += int64(columnBytes)
~~~

### 7.3 MsBuilder

当前 MsBuilder 在真正写盘前通过 cm.size 推进 dataOffset。应改为写后使用物理 DataSize：

~~~text
1. 初始化文件 header，确定 cm.offset。
2. encode chunk，得到 uint64 provisional size。
3. 写 data，检查 error 和 short write。
4. chunkEnd = writer.DataSize()。
5. actual = uint64(chunkEnd - cm.offset)，checked。
6. actual 与 encoder provisional size 交叉校验。
7. cm.size = actual。
8. dataOffset = chunkEnd。
9. checked marshal ChunkMeta。
~~~

首个 chunk 的 tableMagic/version 可能与 chunk data 一起处于 encode buffer 中，但不属于 cm.size。实际长度必须用 chunkEnd - cm.offset，而不是直接使用首次写前的 writer.DataSize()。

写失败时不能推进 dataOffset，也不能写出对应 ChunkMeta。

### 7.4 stream writer

stream compact、stream downsample 和 raw writer 的物理 cursor 已主要依赖 writer.DataSize()，但 metadata 出口仍可能显式转成 uint32。

这些路径应：

- 在内存中设置完整 uint64 cm.size；
- 只在统一 marshal 边界写 low32；
- 对 Legacy32 policy 在 marshal 前拒绝 large output；
- 不允许在业务逻辑中保留 uint32 totalSize。

### 7.5 文件切分

文件大小判断使用 int64 DataSize。一个 SID chunk 已经开始后不能因为越过 fileSizeLimit 而把同一 ChunkMeta 拆到两个文件；应完成当前 chunk，并从下一 SID 开始新文件。

### 7.6 snapshot 内存边界

cm.size 宽化解决的是数值和格式解释，不会自动降低 snapshot 的内存峰值。当前 buffered MsBuilder 仍可能同时持有 memtable Record 和完整 encoded chunk。

本方案不再用 shard 级 ErrValueTooLarge 作为 uint32 保护，因此多 series 的 GB 级 resident 数据不会因为“整个 shard 接近 4 GiB”被格式层拒绝。但 R2 要宣称可以生产启用 large writer，必须同时提供有界的 segment-streaming snapshot；只完成 uint64 类型修改时，writer gate 仍保持关闭。

snapshot 保留双路径：

~~~text
预计 encoded chunk <= MaxBufferedChunkBytes
    -> 现有 buffered MsBuilder fast path

预计 encoded chunk > MaxBufferedChunkBytes
    -> segment-streaming snapshot
~~~

路由必须发生在构造完整 encodeChunk 之前。估算至少覆盖 payload、offset、bitmap、header、CRC 和编码器 overhead；阈值是 transient memory 参数，不是 uint32 格式上限。

segment-streaming 路径：

1. chunkStart 取 writer.DataSize()，设置 cm.offset。
2. 按现有字段和 time 的物理顺序处理，每列先写既有格式的 4 字节 CRC region。
3. 每次只把一个 Segment 编码到可复用 buffer；Segment.size 最终 checked narrow。
4. Segment.offset 取写入前的 DataSize，写完立即释放/复用 buffer。
5. 增量维护列 pre-aggregation、timeRange 和 CRC 语义；优先复用现有 StreamWriteFile/ColumnBuilder 的约定，不引入新线格式。
6. 完成全部字段和 time 后，chunkEnd 取 DataSize。
7. cm.size 设置为 uint64(chunkEnd - chunkStart)，再走统一 checked marshal。

streaming 路径不改变当前 sort、duplicate timestamp、last-non-null、ordered/unordered 分流等上游语义；它只替换 encoded chunk 的落盘方式。相关 Record 语义继续使用现有实现和回归 fixture。

资源侧还必须满足：

- 生产能力只在 64 位 Go 架构开启；
- snapshot 并发和 transient buffer 受现有 node/shard 资源预算约束；
- buffered 路径的峰值按最大允许 fast-path chunk，而不是只按单批写入估算；
- streaming 路径的增量 encoded 工作集受最大单 Segment buffer 和 writer staging buffer 限制；
- route 所需估算失败或溢出时保守选择 streaming，不能回退到无界 buffered encode。

资源不足应返回可重试资源错误并由 snapshot 调度处理，不能重新用全 shard 的 uint32 上限拒绝正常多 series 写入。

---

## 八、query 读取

### 8.1 preload 决策

unmarshal 完成后 cm.size 已经是真实长度。query 继续保留小 chunk 整块预读优化：

~~~text
if cm.size > 0
   && cm.size < defaultIoSize
   && cm.size <= MaxUint32:
       preload [cm.offset, cm.offset + cm.size)
else:
       按 Segment.offset / Segment.size direct read
~~~

large chunk 一定直接按 Segment 读取，不把 ReadDataBlock 的 size 参数机械升级为 uint64，也不为 query 分配多 GiB buffer。

### 8.2 checked window

当前 columnData 直接进行切片。应替换为 error-returning helper：

~~~go
func SegmentFromWindow(
    window []byte,
    windowStart int64,
    seg Segment,
) ([]byte, error)
~~~

切片前检查：

- seg.offset 不小于 windowStart；
- offset 和 end 使用 checked arithmetic；
- end 不超过 window 长度；
- Segment 同时位于 cm.DataRange() 和 trailer data range 内。

任何异常都返回错误，禁止 panic。

### 8.3 一次 direct fallback

在有效文件中，真实 cm.size 小于 defaultIoSize 时，preload 应覆盖所有 Segment。窗口 miss 或首次 decode 失败属于异常兜底，而不是 size 回绕的正常主流程：

1. 丢弃本次 attempt 的 scratch Record 和 decoder 临时状态。
2. 释放 preload cache page。
3. 使用 Segment.offset/size direct read 一次。
4. direct 成功后原子提交结果，并记录文件级 warning/audit metric。
5. direct 再失败时返回真实 I/O/corruption error并发送重要告警。

不能在部分列已经写入最终 Record 后切换 fallback，否则会产生重复列或半条记录。

### 8.4 范围校验

所有 query Segment read 同时检查：

~~~text
chunkStart = cm.offset
chunkEnd   = checked(cm.offset + cm.size)

chunkStart <= seg.offset
seg.offset + seg.size <= chunkEnd
trailer.dataStart <= seg.offset
seg.offset + seg.size <= trailer.dataEnd
~~~

DecodeColumnHeader、decodeColumnData、appendTimeColumnData 等函数还要在读取 data[0] 前检查最小输入长度。

---

## 九、whole-chunk consumer

### 9.1 总体规则

cm.size 升级后，现有使用点会出现编译错误，但“依赖编译器修复”只负责发现类型不匹配。以下转换即使能编译也被禁止：

~~~go
readSize := uint32(cm.size)
buf := make([]byte, int(cm.size))
end := cm.offset + int64(cm.size)
~~~

除统一 marshal 边界外，uint32(cm.size) 只能在已经证明 cm.size 不超过 MaxUint32 的分支中出现。

whole-chunk consumer 必须先选择能力：

- small chunk：保留当前整块读取 fast path。
- large chunk 且支持 segment/stream：使用有界工作集处理。
- large chunk 且当前任务不支持：在读取、分配和产生输出前返回 ErrLargeChunkUnsupported；只有后继 stream 路径已声明安全时才能返回 ErrRequireStream。

不能只拓宽 ReadData API 后一次性申请 cm.size 大小的内存。

### 9.2 ChunkIterator、compact 和 export

ChunkIterator 当前按 cm.offset/cm.size 整块读取。改造方式：

1. 在读取前检查真实 size 和任务工作集上限。
2. 未超限时 checked narrow 后沿用现有 fast path。
3. 超限时路由到已有 stream/segment 处理；该任务尚无安全路径时返回 typed capability error。
4. iterator 必须把首次和后续错误传给任务入口，不能把错误解释为 EOF 或空文件。

fast/nonstream compact、merge_self、tssp-to-parquet 和 consume/tsreader 都需要逐一确认。Parquet/export 如果没有 segment 流式实现，应明确报不支持，而不是静默输出不完整结果。

merge streamMode 的数值和工作集优化属于独立工作项，本方案不修改该实现，也不把它作为 ErrRequireStream 的默认目标路径。在独立能力完成前，可能触发该路径的 shard/task 不开放 large writer；所需 gate 由对应独立工作项交付。

### 9.3 WriteOriginal

WriteOriginal 可以直接使用内存中的真实 cm.size：

~~~text
sourceStart = sourceMeta.offset
remaining   = sourceMeta.size
targetStart = writer.DataSize()
~~~

复制流程：

1. 在修改任何 metadata 前校验 source range。
2. deep clone source ChunkMeta。
3. 使用固定小 buffer 循环复制；每次 I/O 长度 checked narrow 到 uint32。
4. 检查 short read 和 short write。
5. remaining 使用 uint64，source/target cursor 使用 checked int64。
6. 以 targetStart - sourceStart 重定位 clone 中所有 Segment.offset。
7. 设置 clone.offset=targetStart，clone.size 保持真实长度。
8. 写出 clone metadata。

禁止原地修改 FileIterator/cache 持有的 source meta。现有 Clone 未完整复制 preAgg 和 timeRange，必须一并修复。

### 9.4 BufferReader

BufferReader.reset 当前可能先把 int64 offset delta 转为 uint32。应先在 int64 上判断：

~~~go
delta := offset - br.offset
if delta < 0 || delta > int64(br.size) {
    br.resetWindow(offset)
    return
}
n := uint32(delta)
~~~

只有已经证明 delta 位于当前小窗口时才允许 narrow。

---

## 十、历史文件与告警

### 10.1 可恢复情况

满足以下条件的文件可以恢复：

- ChunkMeta 磁盘 size 发生低 32 位回绕；
- cm.offset 和所有 Segment.offset 仍指向真实物理位置；
- 列区间连续；
- uint32(恢复出的真实 size) 等于 wireSizeLow。

新 reader 会在 unmarshal 阶段得到真实 cm.size，后续 query 和 consumer 不再感知磁盘回绕。

### 10.2 不可自动恢复情况

如果历史 MsBuilder 已经用回绕后的 size 推进下一 SID，后续 cm.offset 和 Segment.offset 可能整体向前偏移 4 GiB。局部 metadata 可能仍然自洽，仅重新计算 size 不能修复。

随机 query 只持有当前 ChunkMeta 时不保证立即识别这种整体偏移；它可能直到 Segment decode/CRC 失败才发现。因此历史文件检测必须由 R1 文件级顺序审计闭环，不能把查询 fallback 当作唯一检测机制。

文件级处理策略：

- 顺序审计校验 previousEnd == nextChunk.offset。
- 最后一个 chunk 校验 end == trailer data end。
- 校验 Segment decode 和 CRC。
- 发现污染后返回 ErrCorruptTSSP，隔离文件并发送重要告警。
- 不猜测应补回多少个 2^32，也不在 query 热路径原地修 metadata。
- R2 对目标 shard 开放 large writer 前，所有存量 TSSP 必须完成审计；审计结果按 file identity、size 和校验标识记录，文件变化后失效。
- 未完成审计的历史文件可以按现有兼容策略读取，但不得据此宣称已经证明没有 absolute-offset 污染。

### 10.3 告警分级

| 场景 | 行为 |
|---|---|
| actual <= MaxUint32 且 low32 一致 | 正常，无告警 |
| actual > MaxUint32 且 low32 一致、范围合法，且文件由 R2 writer 生成 | 正常 large chunk；记录容量指标，不发送异常告警 |
| actual > MaxUint32 且 low32 一致，但文件来源/能力标记未知 | 可读取，发送去重提示并调度文件审计 |
| low32 不一致、区间有洞/重叠、越 trailer | 返回 ErrCorruptTSSP，重要告警 A |
| query preload 异常但 direct read 成功 | 返回数据，warning，并调度文件审计 |
| direct read/decode 仍失败 | 不返回部分结果，重要告警 A |
| 相邻 chunk 边界不连续 | 视为绝对 offset 污染，重要告警 A并隔离 |

告警必须按 file identity、SID 和错误类型去重/限频，避免同一坏块被查询流量放大。

---

## 十一、兼容性与发布

### 11.1 兼容矩阵

| 文件/版本组合 | 结果 |
|---|---|
| 旧版普通文件 -> 新 reader | 兼容；新 reader恢复出同样的真实 size |
| 新版普通文件 -> 旧 reader | 兼容；磁盘字节和值保持不变 |
| 新版 large TS attached 文件 -> 新 reader | 支持 |
| 新版 large TS attached 文件 -> 旧 reader | 不支持；旧 reader仍把低 32 位当真实长度 |
| 历史绝对 offset 污染文件 -> R1 文件级审计 | 检测并隔离，不承诺自动恢复；随机 query 不保证打开即识别 |
| 新版 Legacy32 layout -> 旧 reader | 兼容，writer仍禁止 large chunk |

### 11.2 reader-first

建议分两阶段发布。

R1：读能力和类型迁移

- ChunkMeta.size 全局升级为 uint64。
- 两套 codec、selected decoder 和 TS attached size resolver 上线。
- query、whole-chunk consumer、WriteOriginal 和 BufferReader 完成安全改造。
- 所有 writer 仍保持 LegacyChunk32 发布 gate。
- 对历史文件执行顺序审计并记录 verified inventory。

R2：TS attached large writer

- 确认所有可能读取目标文件的 query、compact、merge、export、备份和运维角色已具备 R1。
- segment-streaming snapshot 已完成，large chunk 不再依赖完整 encodeChunk buffer。
- 仅对 TSStore attached writer 解除 LegacyChunk32 gate。
- ColumnStore、detached 等保持原 gate。
- 启用前记录集群/shard capability，阻止旧 reader 接管。

### 11.3 回滚

- 只要已经存在 large 文件，旧 reader 就不能接管。
- 关闭 writer feature flag 只能阻止新 large 文件，不能消除既有文件要求。
- 回滚前要检查 active/sealed memtable、待 flush 数据、后台 rewrite 和已生成文件。
- 回滚到不具备 R1 的版本前，必须先离线重写或移除全部 large 文件，并完成审计。

WAL replay 峰值优化仍是独立工作项；若某部署场景可能在 replay 阶段形成当前版本无法承受的工作集，应通过部署 gate 管理，不在本文修改 replay。

---

## 十二、实施拆分

### 12.1 PR 1：ChunkMeta uint64 与 codec

- 修改 ChunkMeta.size 类型。
- 保持 ChunkMeta.Size() 原语义。
- 引入 ChunkSizePolicy，并从 engine/table 上下文传入、固化到 file reader。
- 修改 fixed/self-compressed marshal、unmarshal。
- 覆盖 DetachedChunkMetaReader 和其他绕过 adaptive decoder 的入口。
- 收敛所有 cm.marshal 旁路。
- Legacy32 writer 增加最终断言。
- 依赖编译器修复全仓基础类型错误，但单独审计所有显式 uint32(cm.size)。

### 12.2 PR 2：TS attached resolver 与 reader

- full/selected decoder 完整扫描 wire ColumnMeta。
- 计算真实 uint64 size 并校验 low32。
- query 使用真实 size 决定 preload。
- columnData 改为 checked window。
- Segment direct read 增加 chunk/trailer range 校验。
- 加入历史回绕提示告警和 corruption 重要告警。

### 12.3 PR 3：writer 与 consumer

- ChunkDataBuilder 列/chunk 累计升级。
- MsBuilder 写后通过 DataSize 确认 size 和下一游标。
- 增加 buffered/segment-streaming snapshot 路由，large 路径不构造完整 encodeChunk。
- stream writer metadata 出口修复。
- WriteOriginal uint64 remaining、deep clone 和分块复制。
- ChunkIterator/compact/export 能力分流。
- BufferReader 和 iterator error propagation 修复。

三个 PR 可以独立评审，但生产启用 large writer 必须满足 R1/R2 顺序。

---

## 十三、主要改动文件

| 文件 | 主要改动 |
|---|---|
| engine/immutable/tssp_file_meta.go | ChunkMeta.size uint64、fixed codec、clone/reset/accessor |
| engine/immutable/chunk_meta_codec.go | self-compressed codec、selected 全量扫描 |
| engine/immutable/tssp_reader.go | 打开文件时接收并固化 engine/layout policy |
| engine/immutable/tssp_file.go | decode policy、真实范围、query preload、checked Segment read |
| engine/immutable/sequence_iterator.go | 只选择 value 时仍恢复完整 size |
| engine/immutable/detached_chunkmeta.go | DetachedChunkMetaReader 的 Legacy32 checked decode |
| engine/immutable/tssp_file_detached_reader.go | 传递并固化 detached layout policy |
| engine/immutable/segment_sequence_reader.go | detached sequence decode policy |
| engine/immutable/chunkdata_builder_ts.go | 列 aggregate delta 和 chunk 累计宽化 |
| engine/immutable/chunkdata_builder.go | time Segment checked narrow、uint64 chunk 累计 |
| engine/immutable/msbuilder.go | 写后 DataSize 游标、checked marshal、detached Legacy32 |
| engine/mutable snapshot 调度与 flush 入口 | buffered/segment-streaming 路由和 transient budget |
| engine/immutable/stream_compact.go | metadata 出口不提前截断 |
| engine/immutable/stream_downsample.go | metadata 出口不提前截断 |
| engine/immutable/chunk_iterators.go | whole-chunk 阈值和错误传播 |
| engine/immutable/file_iterator.go | uint64 range/BufferReader delta |
| engine/immutable/merge_performer.go | WriteOriginal 分块复制和 deep clone |
| compact、merge_self、task_parquet、task_cs_parquet 等入口 | 显式 layout policy、ErrRequireStream/capability error 传播 |

---

## 十四、测试与验收

### 14.1 codec

- ChunkMeta.size 的 0、MaxUint32-1、MaxUint32、MaxUint32+1、2^32、2^32+n 边界。
- fixed/self-compressed 普通文件 byte-for-byte golden。
- 两套 marshal 对 large size 都只写 low32。
- self-compressed wire size 大于 MaxUint32 时拒绝。
- Legacy32 policy 对 large size 拒绝 marshal。
- DetachedChunkMetaReader 等旁路只接受 Legacy32，并保持普通文件 size 无损扩宽。
- ChunkMeta.Size() 的 metadata sizing 回归。
- 所有 direct cm.marshal 旁路均经过 policy 校验。
- file reader 缺少 layout policy 时打开失败，query/task 不能覆盖已固化 policy。
- TS/CS Parquet 和 detached reader 分别取得正确 policy。

### 14.2 resolver

- full metadata 恢复普通和跨 4 GiB size。
- ColumnMeta 数组顺序与物理顺序不同。
- selected decoder 只物化一列，但恢复出的 size 与 full decoder 完全一致。
- sequence iterator 只请求 value。
- size 恰为 2^32 时 wireSizeLow 为 0。
- CRC gap 不是 4、Segment 乱序、列间洞/重叠、加法溢出、越 trailer、low32 mismatch 全部返回 ErrCorruptTSSP。
- 相邻 chunk end/offset 不一致被审计识别。
- fuzz 输入不 panic、不返回半初始化 ChunkMeta。

### 14.3 writer

- 多个合法 Segment 使单列累计跨过 4 GiB，下一列 offset 仍正确。
- 多列累计使 chunk 跨过 4 GiB，cm.size 为真实 uint64。
- MsBuilder 下一个 SID offset 等于实际 writer.DataSize。
- 首 chunk header 不计入 cm.size。
- data short write/error 后不推进游标、不写 metadata。
- stream writer 和 buffered writer 生成一致 metadata。
- streaming snapshot 峰值 encoded buffer 受单 Segment 上限约束，不随 chunk 总长度增长。
- buffered/streaming 在相同 Record 上的查询结果、preAgg、timeRange 和 duplicate timestamp 语义一致。
- Segment.size 超限返回 ErrSegmentTooLarge。

大边界测试应使用 synthetic metadata、counting writer、稀疏文件或可配置小位宽测试器，避免 CI 实际分配 4 GiB 内存。

### 14.4 query

- 普通小 chunk 继续命中一次 preload。
- large chunk 不执行整块 preload，按 Segment 读取。
- checked window 在短读、错误 offset、错误 size 下返回 error而非 panic。
- preload miss 后 direct read 只重试一次。
- fallback 前已解码的 scratch 不进入最终 Record。
- direct read 同时受 chunk range 和 trailer range 约束。
- decoder 对空/短 header 不访问 data[0]。

### 14.5 consumer

- ChunkIterator 在 large size 下不会发生 uint32 截断或超大无界分配。
- 不支持路径在任何输出前返回 typed error。
- 首次 iterator error 不被当成 EOF。
- WriteOriginal 以固定 buffer 复制跨 4 GiB range，remaining 不回绕。
- WriteOriginal 不修改 source meta，preAgg、timeRange 和 entries 均完整复制。
- BufferReader 遇到相距 4 GiB 以上的 offset 时重置窗口。
- Parquet/export 不支持时显式报错，不生成部分文件。

### 14.6 兼容与发布

- 新 reader 读取现有普通 TSSP fixture。
- 新 writer 普通 chunk 可由旧 reader 读取。
- R1 writer 无法发布 large chunk。
- R2 只对 TS attached 解除 gate。
- 旧 reader 无法接管标记为需要 large-reader 的 shard/file。
- 历史仅 size 回绕 fixture 可恢复。
- 历史 absolute offset 污染 fixture 被文件级审计告警和隔离；随机 query 不承担完整识别承诺。

### 14.7 性能

- selected decoder 全量扫描 metadata 前后的 CPU、allocation 和 p99。
- 小 chunk query preload 命中率和延迟无明显回退。
- large query 的 Segment direct read 次数和缓存命中。
- WriteOriginal 固定 buffer 大小与吞吐。
- compact/merge capability 分流不增加普通 chunk fast path 开销。

---

## 十五、完成标准

以下条件全部满足后，才能启用 TSStore attached large writer：

1. ChunkMeta.size 在所有已构造对象，以及从格式有效且通过 layout policy 校验的文件中解码出的对象里，都只表示真实 uint64 data length。
2. TS attached full、selected 和 sequence decode 都扫描完整物理 metadata；不存在把 wire low32 暴露给调用方的入口。
3. ColumnStore、detached 等 Legacy32 路径完成无损扩宽和最终 size 上限断言。
4. fixed/self-compressed codec 对普通数据保持原字节，对 large TS attached 统一写 low32。
5. writer 的列累计、chunk 累计、copy remaining 和文件 cursor 均无 uint32 中间截断。
6. 下一 SID 起点来自实际 writer.DataSize，首 chunk header 计算正确。
7. large snapshot 走有界 segment-streaming 路径，不构造完整 encodeChunk。
8. query large chunk 只做 Segment direct read，所有 window/slice/range 均 checked。
9. whole-chunk consumer 要么有有界处理路径，要么在读取和输出前 fail closed。
10. WriteOriginal 使用真实 cm.size、uint64 remaining、固定 buffer 和完整 deep clone。
11. 仅 size 回绕文件可恢复；absolute offset 污染由文件级审计识别并隔离。
12. reader-first 已覆盖所有会读取目标文件的角色，旧 reader 被 capability gate 阻止。
13. WAL replay 和 merge streamMode 的独立风险在部署 gate 中明确，不被本方案错误宣称为已解决。
