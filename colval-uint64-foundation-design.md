# ColVal uint64 基础迁移与 32 位边界适配设计

> 文档状态：设计稿，代码尚未实施。
>
> 适用阶段：ColVal uint64 基础 PR。
>
> 关联文档：[`uint64-colval-offset-overflow-fix-design.md`](./uint64-colval-offset-overflow-fix-design.md)、[`tssp-large-chunk-enablement-design.md`](./tssp-large-chunk-enablement-design.md)。
>
> 文档关系：本文只负责内存类型宽化和既有 32 位边界适配；允许 TSSP large chunk 的 ChunkMeta、reader、query、compact/merge 和 snapshot 方案由 `tssp-large-chunk-enablement-design.md` 负责。

---

## 一、结论

record.ColVal.Offset 可以从 []uint32 升级为 []uint64，并作为一个独立的基础变更交付，但必须满足以下条件：

1. 这是一次全仓原子迁移，不是只修改 record 和 packString/unpackString。
2. TSSP、Record、Shelf、executor remote codec、Arrow 和 index ABI 的既有 32 位格式保持不变。
3. 所有 64 -> 32 窄化均显式校验并可返回错误，禁止静默截断。
4. 基础版本继续执行 LegacyChunk32 策略：不得生成 encoded size 大于 MaxUint32 的 TSSP chunk。
5. 本文不声明 large chunk 可写、可读、可 compact，也不使用 ChunkMeta.size 的低 32 位表达真实大长度。

因此：

- 作为“内部表示宽化和边界清理”可以独立合并、测试；只有强制执行 `LegacyChunk32` 时才能独立发布。
- 作为“允许单个 TSSP chunk 达到 4GiB（即超过 `MaxUint32`）”的完整修复不成立。
- 如果不能证明 writer 在基础版本中永远不会生成 large chunk，则该版本不能承担 write role。

对当前“大 String、多 series、短时 GB 级写入”的目标负载，不建议把带保守 `LegacyChunk32` gate 的 foundation 当作长期独立生产版本；推荐独立开发和合入，但按总览中的 reader-first 计划与 large-chunk 能力连续交付。

空新集群可以省去历史污染文件迁移和旧 WAL replay 兼容问题，但不能省去新数据首次写入时的边界校验。新集群同样可能在运行后形成达到 4GiB（即超过 `MaxUint32`）的单 SID chunk。

---

## 二、术语和边界

### 2.1 Wide ColVal

Wide ColVal 指进程内：

~~~go
type ColVal struct {
    Val          []byte
    Offset       []uint64
    Bitmap       []byte
    BitMapOffset int
    Len          int
    NilCount     int
}
~~~

uint64 只表示进程内 offset 能力，不表示任何 wire、文件或 C ABI 已升级到 64 位。

### 2.2 Legacy 32 位边界

以下边界继续保持原格式：

- TSSP String V1/V2 的 payload length、row count、offset/length。
- TSSP Segment.size 和 ChunkMeta.size。
- Record V1 codec 的 offset slice 和子块长度。
- Arrow String 的 signed int32 offset。
- Shelf WAL 的 uint32 raw offset。
- executor V1 remote codec 的 uint32 offset。
- tokenizer 的 int32 offset/length。
- fulltext C ABI 的 uint32 offset/length。
- consume/Kafka 和其他复用 Record codec 的外层 uint32 frame。

### 2.3 Large chunk

本文将以下情况定义为 large chunk：

~~~text
actual encoded bytes of one TSSP ChunkMeta > MaxUint32
~~~

基础版本不允许产生该文件。

这与 Wide ColVal 是两个独立概念：

- 内存结构使用 uint64。
- 文件能力仍限制为 LegacyChunk32。
- 某个临时 ColVal 即使能表达超过 uint32 的 offset，也不代表它能被基础版本直接持久化或发送到 legacy boundary。

### 2.4 运行架构前提

本文的生产支持范围是 64 位 Go 架构（首要目标为 Linux amd64；其他 64 位 GOARCH 需通过同等测试）。`ColVal.Val` 的长度和 slice index 仍由 Go `int` 表示，因此：

- write role 启动时必须确认 `strconv.IntSize == 64`；32 位进程不得声明 `ColValOffsetU64` 持久化能力。
- uint64 offset 只扩大逻辑算术范围，不允许直接作为未经校验的 slice index。
- 每次 uint64 -> int 转换前必须同时检查 `offset <= uint64(len(Val))` 和 `offset <= uint64(MaxInt)`。
- CI 中的 Linux amd64 任务是验证手段，不替代该产品前提。

---

## 三、范围与非目标

### 3.1 本文覆盖

- ColVal.Offset 的 []uint32 -> []uint64 全仓迁移。
- String 和 Tag 的 append、slice、split、sort、merge、update/delete、validate。
- TSSP String V1/V2 的 checked narrow 和 decode widen，wire byte 不变。
- Record V1 codec、measurement wrapper、WAL/gRPC/consume adapter。
- Arrow String 输入和输出适配。
- executor 内存 offset 和 V1 remote codec 适配。
- Shelf WAL、skip index、fulltext/tokenizer C ABI 适配。
- ColumnStore、detached 等共享 ColVal 路径的编译和 legacy boundary 适配。
- 内存计量、错误传播、静态门禁、测试和发布边界。
- LegacyChunk32 的准入和最终 writer 断言契约。

### 3.2 明确不覆盖

- 不修改 TSSP version 或 String encoding version。
- 不改变 ChunkMeta 的磁盘字段或解释语义。
- 不引入 sizeLow、actualSize 或 full/partial ChunkMeta state。
- 不设计 query preload fallback 或真实 chunk range 恢复。
- 不设计 large chunk compact、merge、downsample、raw copy。
- 不设计 streaming snapshot、snapshot generation 或 large writer 工作集。
- 不修复历史版本已经写错的绝对 Segment.offset。
- 不承诺单个不可拆 String value 超过任一 legacy ABI 上限时仍可写入。
- 不以本文为依据移除 LegacyChunk32 gate。

涉及上述能力时，必须转交 `tssp-large-chunk-enablement-design.md`，不在本文追加局部特例。

---

## 四、基础版本的发布不变量

### 4.1 内存不变量

对 String/Tag ColVal：

~~~text
len(Offset) == Len
0 <= Offset[i] <= Offset[i+1] <= uint64(len(Val))
Offset[i] 表示第 i 个逻辑行的起点，null 行也占一个 offset
最后一个逻辑值的结束位置为 uint64(len(Val))
base + localOffset 使用 checked uint64 add
slice 前验证 offset/end，再转换为 int
~~~

对非 String/Tag 列，Offset 必须为空。

### 4.2 Wire 不变量

基础版本写出的普通数据必须与旧格式 byte-for-byte 兼容：

~~~text
TSSP String       big-endian uint32
Record V1         little-endian uint32 offsets
Shelf WAL         既有 native/raw uint32 layout
executor V1       既有 uint32 offset layout
Arrow String      signed int32 + N+1 sentinel
~~~

禁止为方便共享 helper 而统一这些字节序。

### 4.3 LegacyChunk32 不变量

基础版本继续要求：

~~~text
actualEncodedSegmentSize <= MaxUint32
actualEncodedChunkSize   <= MaxUint32
legacy frame size        <= 对应 wire 字段上限
~~~

ChunkMeta.size 在本文中仍表示真实 chunk 长度的 uint32 值。禁止：

~~~go
cm.size = uint32(actualSize) // actualSize > MaxUint32
~~~

也禁止把回绕后的低 32 位解释为受控语义。

### 4.4 两层保护

仅在最终 snapshot 编码时发现 chunk 过大是不够的，因为数据可能已经向客户端确认。基础版本必须同时具备：

1. mutation 前的保守准入证明。
2. writer 发布前的实际长度断言。

mutation 前可使用整个 active generation 的保守 encoded upper bound。因为任一单 SID chunk 不可能大于整个 generation，该上界小于等于 MaxUint32 即可证明 LegacyChunk32。

~~~text
projectedGenerationEncodedUpperBound(current + request) <= MaxUint32
~~~

要求：

- 上界包含 payload、offset/length、bitmap、column header、segment header、CRC 和压缩器 worst-case overhead。
- 不能只使用当前 Record.Size() 或配置中的 nominal memtable size。
- 一个请求可能让 generation 越界时，必须在任何 mutation 前完成 rotate/seal；如果当前基础版本无法同步完成，则返回 typed retryable error。
- 空 generation 中单个不可拆请求仍超过上界时，返回永久边界错误。
- 不允许先修改部分 measurement/row，再因另一个列超界而失败。

writer 最终使用实际 encoded delta 检查 segment 和 chunk。最终检查失败时不得发布文件，并触发高优先级告警；在正确的 admission gate 下该错误应不可达。

本文只规定上述安全契约，不展开 rotate、snapshot 调度和重试实现。

整个 generation 的 upper bound 是便于证明正确性的保守基线，不是目标业务的稳态性能方案；若其轮转频率不可接受，就不能把 foundation 单独部署到 write role，而应完成 large-chunk reader-first 闭环后再启用对应 writer。

### 4.5 启动和部署 gate

write process 启动前必须证明 LegacyChunk32 admission 生效。以下任一情况成立时不得承担 write role：

- active generation 没有严格上限。
- 单次请求可以绕过 reservation 后把 generation 推过上限。
- encoded upper bound 未覆盖 String/Tag、bitmap 或 codec overhead。
- writer 仍存在 unchecked chunk size cast。

这里的 writer 包含所有 TSSP finalizer：memtable snapshot、compact、merge、downsample、raw copy、repair 和其他 rewrite。只限制前台 ingest、却允许后台任务通过 `uint32(actualSize)` 发布大 chunk，不满足 foundation/R1 安全包络。

只依赖“当前默认配置通常小于 4GiB”不构成证明。

---

## 五、共享 helper 和 API

### 5.1 Checked arithmetic

建议集中提供：

~~~go
func CheckedAddUint64(a, b uint64, boundary string) (uint64, error)
func CheckedUint8(v uint64, boundary string) (uint8, error)
func CheckedUint16(v uint64, boundary string) (uint16, error)
func CheckedUint32(v uint64, boundary string) (uint32, error)
func CheckedInt32(v uint64, boundary string) (int32, error)
func CheckedInt(v uint64, upper int, boundary string) (int, error)
~~~

错误至少包含：

~~~text
boundary name
observed value
limit
~~~

### 5.2 ColVal helper

~~~go
func IsVarLen(typ int) bool
func ValidateVarCol(col *ColVal) error
func ValueRange(col *ColVal, row int) (start, end int, err error)
func RebaseOffsets(dst, local []uint64, base uint64) ([]uint64, error)
~~~

IsVarLen 必须同时覆盖 String 和 Tag。不得继续在共享逻辑中只判断 Field_Type_String 或 Field.IsString()。

### 5.3 32 位数值转换

~~~go
func NarrowOffsetsU32(
    dst []uint32,
    src []uint64,
    valLen int,
    boundary string,
) ([]uint32, error)

func WidenOffsetsU32(dst []uint64, src []uint32) []uint64

func NarrowOffsetsI32(
    dst []int32,
    src []uint64,
    valLen int,
    boundary string,
) ([]int32, error)

func WidenOffsetsI32(
    dst []uint64,
    src []int32,
    boundary string,
) ([]uint64, error)
~~~

规则：

- U32 和 I32 必须分开，不能用“32 位”作为模糊上限。
- narrow 校验单调性、每项不超过 valLen，以及 valLen 自身能被目标 ABI 表示。
- I32 decode 校验非负。
- helper 只转换数值，不负责 TSSP/Record/Shelf 的字节编码。
- 不允许 unsafe 将 []uint64 解释为 []uint32 或 []int32。

### 5.4 Size 语义

必须区分内存大小和 wire 大小：

- Record.Size() 是内存规模，offset 按 8 字节。
- ColVal 当前的 Size() 是 Record V1 wire 大小，offset 仍按 4 字节；建议重命名为 CodecSizeV1()。
- Record.CodecSize() 继续使用 V1 wire 大小。
- executor ColumnImpl.Size() 继续表示 executor V1 wire 大小，offset 仍按 4 字节。

机械地把所有乘 4 改成乘 8 会破坏子块 framing；完全不改内存计量又会低估 resident memory。

---

## 六、ColVal 全生命周期改造

### 6.1 Append

所有 String/Tag append 使用 uint64 base：

~~~go
base := uint64(len(dst.Val))
~~~

从另一个 ColVal 追加 row range 时必须：

1. 校验 source row range。
2. 校验 source offset 和 bitmap。
3. 计算 source value byte range。
4. checked 计算目标 offset。
5. 全部成功后一次提交 Val、Offset、Bitmap、Len 和 NilCount。

推荐采用 preflight -> commit：

~~~go
plan, err := PlanAppendVarCol(dst, src, start, end)
if err != nil {
    return err
}
ApplyAppendVarCol(dst, src, plan)
~~~

失败后 dst 必须 byte-for-byte 不变。已有 void API 不能吞掉错误；可信热路径必须有显式 validated 前置条件。

### 6.2 Null 语义

ColVal.Offset 与逻辑行一一对应，null 行也有 offset。不得把 executor 的 valid-only offset 语义反向套用到 ColVal。

RemoveNilOffset 只能生成独立 view，不能原地修改共享 Record。当前 Record -> executor adapter 中修改 recColumn.Offset 会破坏 retry、fallback 和后续消费者，迁移时必须消除。

### 6.3 Split 和 rebase

Split、SplitColBySize、SliceFromRecord 和 segment-local view 必须把第一个 value offset 归零：

~~~text
localOffset[i] = sourceOffset[start+i] - sourceOffset[start]
~~~

追加 segment decode 结果到完整列时使用目标 Val 长度重新基准化：

~~~text
dstOffset = uint64(len(dst.Val)) + segmentLocalOffset
~~~

Segment.offset 是文件物理位置，禁止参与 ColVal logical offset rebase。

### 6.4 Sort、merge、update/delete

- sort/merge 的目标 base、累计 offset 和临时 slice 全部使用 uint64。
- update/delete 的长度差使用有符号 checked arithmetic；禁止把负 int 直接转换为 uint64。
- duplicate、null precedence 和 bitmap 语义保持原行为。
- 操作完成后重新校验 offset 单调性和末端范围。
- AppendAll 只允许目标为空；非空目标必须使用 rebase append。

### 6.5 内存计量

offset 从 4 字节变为 8 字节：

~~~text
extra memory = 4 * String/Tag logical rows
~~~

至少同步修改：

- Record.Size()。
- mutable write admission 的 resident estimate。
- String/Tag、time、scalar、bitmap 和 slice growth/headroom 预算。
- node/shard mutable memory 指标。

该计量用于真实资源保护和 LegacyChunk32 admission，不表示 allocator 的精确 RSS。

---

## 七、TSSP String V1/V2

### 7.1 Wire 保持不变

packString/unpackString 是必须显式设计的算法边界，不能只依赖编译器修复。

pack 接受 []uint64 或先调用 NarrowOffsetsU32，最终仍写原 big-endian uint32：

~~~text
V1:
byteLen:uint32
bytes
offsetBytes:uint32
offsets:uint32[]

V2:
version:uint32
byteLen:uint32
bytes
rowCount:uint32
lengths:uint32[]
~~~

### 7.2 Encode 校验

编码前检查：

- len(in) <= MaxUint32。
- V1 的 `offsetBytes` 写入 uint32，因此必须 checked 计算 `len(offset)*4 <= MaxUint32`，等价于 `len(offset) <= MaxUint32/4`。
- V2 的 row count 必须 `<= MaxUint32`，并在分配/写入前 checked 计算 `lengthArrayBytes=len(offset)*4`；length array 和完整 compressed frame 还必须满足 segment/frame 上限。
- offset 单调，且每项 <= len(in)。
- 每个相邻 value length 和最后一个 value length 可由 uint32 表示。
- String compressor 的 source length、compressed length 和 block length 可由既有字段表示。
- 最终 Segment encoded size <= MaxUint32。

任何检查必须发生在写对应长度字段之前。

### 7.3 Decode 校验

V1：

- 读取 uint32 前检查最小长度。
- offsetBytes 必须是 4 的倍数。
- offsetBytes 必须恰好消费剩余 offset payload；不能接受截断或 trailing bytes。
- 每个 uint32 widen 为 uint64。
- 校验首 offset、单调性和 <= byteLen。

V2：

- rowCount*4 使用 checked arithmetic。
- 必须有完整的 length array，禁止只比较剩余字节与 rowCount。
- rowCount==0 时不能写 offset[0]。
- 使用 uint64 累加 lengths。
- 最终累计长度必须等于 byteLen。
- 拒绝 trailing 或截断的 length data。

损坏输入返回 typed corruption error，不能 panic。

### 7.4 Decode 后追加

单 segment decoder 输出 segment-local []uint64 offsets。整 chunk reader 将多个 segment 追加到目标 ColVal 时必须调用 checked rebase append。

基础版本不修改 query 预读和 ChunkMeta 解释；这是因为 LegacyChunk32 gate 保证现有 chunk range 语义仍成立。

### 7.5 Chunk 最终断言

虽然本文不设计 large chunk writer，但所有普通 writer 必须在写 ChunkMeta 前检查实际 chunk delta：

~~~text
actualEncodedChunkSize <= MaxUint32
~~~

成功后才设置现有 ChunkMeta.size。超过边界返回 ErrLargeChunkUnsupported，禁止 low32 cast 和文件发布。

---

## 八、Record、WAL、gRPC 和 consume

### 8.1 Record V1 codec

Record wire 继续保存 uint32 offsets。新增 checked API：

~~~go
func (cv *ColVal) MarshalV1Checked(dst []byte) ([]byte, error)
func (cv *ColVal) UnmarshalV1Checked(
    src []byte,
    typ int,
) (rest []byte, err error)

func (rec *Record) MarshalV1Checked(dst []byte) ([]byte, error)
func (rec *Record) UnmarshalV1Checked(
    src []byte,
) (rest []byte, err error)
~~~

Marshal：

- preflight 全部列后再编码。
- offset 使用 NarrowOffsetsU32。
- 保持原 little-endian value encoding。
- 校验 Val、Bitmap、offset count、schema count、sub-block 和完整 Record frame。
- schema field name 保持既有 uint16 上限。

Unmarshal：

- 检查 count*width、子块长度和剩余 buffer。
- uint32 offsets 逐项 widen。
- 按 schema type 校验 ColVal 形态。
- String/Tag 要求 len(Offset)==Len、单调、不越过 Val。
- 校验 Bitmap、BitMapOffset、NilCount。
- 顶层入口拒绝 trailing bytes。

旧的无 error Marshal/Unmarshal 只能用于已经完成相同 preflight 的内部路径，生产入口最终应收敛到 checked API。

### 8.2 Measurement wrapper

现有 wire 是单个 measurement 加单个 Record：

~~~text
nameLen:uint8
name
RecordV1
~~~

checked API 保持 singular 语义：

~~~go
func MarshalWithMeasurementV1Checked(
    dst []byte,
    measurement string,
    rec *Record,
) ([]byte, error)

func UnmarshalWithMeasurementV1Checked(
    src []byte,
    rec *Record,
) (measurement string, rest []byte, err error)
~~~

校验：

- 1 <= len(measurement) <= MaxUint8。
- decode 时 name bytes 位于剩余输入内。
- 嵌套 Record 完整消费。

不得为了 API 方便改为未带 count/framing 的 measurement/Record slice，否则会形成未版本化的新 wire。

### 8.3 WAL 和 gRPC

- Line protocol WAL 不直接序列化累计后的 ColVal.Offset，格式不因本变更改变。
- Arrow/Record WAL 继续使用 Record V1 checked codec；replay 时 widen。
- gRPC Record decoder 必须使用 checked unmarshal，不能把 panic recovery 当作长度校验。
- 压缩前后的完整 WAL/gRPC frame length 都必须检查，而不仅是单个 ColVal。

空新集群没有旧 WAL，但启动后立即会生成新 WAL，因此 encode/decode 仍必须闭环。

### 8.4 Consume/Kafka

ConsumeRecord 复用 Record V1，同时还要校验：

- Tags count。
- tag key/value 的既有长度字段。
- 单 message 长度。
- 完整 Kafka response frame。

基础版本允许在可保持 row/schema/order 语义时拆分；不能安全拆分时返回 ErrLegacyCodecOverflow。无论选择哪种策略，都禁止 uint32 cast 后继续发送。

如果实现跨 fetch 拆分，必须保存 pending fragment，不能在 Iterator.Next 已推进后丢弃未发送 row。该能力不是 foundation 发布 large data 的承诺，只是 legacy frame 的正确错误/分片语义。

---

## 九、Arrow

### 9.1 输入

Arrow String 使用 signed int32 和 N+1 sentinel。

对 sliced array：

~~~text
base  = Data.Offset
N     = array.Len()
raw   = srcOffsets[base : base+N+1]
first = raw[0]
last  = raw[N]

ColVal.Val       = valueBuffer[first:last]
ColVal.Offset[i] = uint64(raw[i] - first), i in [0,N)
~~~

要求：

- 校验 raw 范围、非负、单调和 last 不越过 value buffer。
- 最后一个 sentinel 只确定 Val 末端，不能写入 ColVal.Offset。
- validity bitmap 复制并按逻辑 row 重对齐到 BitMapOffset=0；非 byte-aligned slice 不能直接截 byte。
- no-null 和 with-null 路径遵循同一 offset 语义。
- 不允许把 Arrow int32 buffer unsafe alias 成 []uint64。

如果 ColVal.Val 继续 alias Arrow value buffer，调用链必须保证 Arrow Record 生命周期覆盖 ColVal 使用期；ColVal 逃逸到异步持有者时必须复制或显式 Retain。

### 9.2 输出

输出标准 Arrow String 时：

- payload、每个 offset 和最终 sentinel 均 <= MaxInt32。
- NarrowOffsetsI32 产生 N 个 row-start offsets。
- 额外 append int32(len(Val)) 形成 Arrow 要求的 N+1 sentinel。
- 超界时按语义安全的 row boundary 拆 array，或返回 typed error。

Go 可以容纳 uint32 不表示 Arrow 可以接受 [2GiB,4GiB) offset。

---

## 十、executor

### 10.1 内存结构

executor ColumnImpl.offset 同步升级为 []uint64。只在 Record -> executor adapter 做一次 narrow 不够，因为 merge、sort、limit 等 transform 之后仍会继续 append。

所有生成模板和生成文件中的以下 API 同步迁移：

- StringValuesWithOffset。
- AppendStringValue/AppendStringBytes。
- SetStringValues/CloneStringValues。
- GetStringBytes。
- split/copy/merge helpers。

executor offset 只对应 valid string values，不对应全部逻辑行：

~~~text
len(offset) == bitmap.length - bitmap.nilCount
~~~

这与 ColVal 的 len(Offset)==Len 不同，validator 不能混用。

### 10.2 Record adapter

当前 adapter 通过 RemoveNilOffset 原地修改 source Record。迁移后必须生成 valid-only 临时 view，保持 source ColVal 不变。

### 10.3 executor V1 codec

remote/executor V1 wire 保持 uint32：

- marshal：U64 -> checked U32。
- unmarshal：U32 -> U64。
- 校验 valid value count、单调性、stringBytes 边界、bitmap、子块和 outer Chunk frame。
- ColumnImpl.Size() 仍按 4-byte wire offset 计算。
- decode 拒绝 trailing bytes。

基础版本不承诺超大 executor output。超过 Arrow/remote frame 边界时可以按 row 拆分或返回能力错误，但不能回绕。

---

## 十一、Shelf、index、ColumnStore 和 detached

### 11.1 Shelf WAL

Shelf 现有格式继续使用 uint32 raw offsets：

- decoder 读取 uint32 后逐项 widen，不能零拷贝赋给 ColVal.Offset。
- encoder checked narrow 后按 Shelf 原 byte order/layout 写出。
- 禁止 Uint32Slice2byte(cv.Offset) 或把 []uint64 直接 unsafe 解释为 bytes。
- 校验单 row、Record、Blob 和 Grouping 的完整 frame。

Blob.WriteRecordRow、GroupingRow、RecordMapper.MapRecord 等无 error API 必须改造错误链，或在调用前完成可证明的完整 preflight。单 row 是不可拆语义单位，超界时返回 ErrLegacyCodecOverflow。

### 11.2 Tokenizer 和 fulltext C ABI

不同 ABI 必须分别处理：

~~~text
tokenizer offsets/lengths    int32
fulltext AddDocument         uint32
~~~

要求：

- 从当前 row/segment range 构造 local Val view。
- offsets rebase 到 0。
- 根据具体 ABI 调用 NarrowOffsetsI32 或 NarrowOffsetsU32。
- signed int32 路径同时检查 value length 和 segment-local payload <= MaxInt32。
- 传 local view 后，C row range 也使用 [0,n)，不能继续传原 Record 全局 start/end。
- 禁止 unsafe.Pointer 指向 []uint64 后交给 uint32_t*；该代码可以编译但 C 会错误解释高低 32 位。

IndexWriter.CreateDetachIndex、GenBloomFilterData 及调用方必须能返回 error。index 失败不得静默生成不完整 index。

### 11.3 ColumnStore 和 detached

共享 ColVal 的 ColumnStore/detached 路径必须完成：

- 编译适配。
- String/Tag uint64 append/rebase。
- TSSP/Record/index legacy boundary checked narrow。
- decode error 后再 commit，禁止先 append 再检查 error。
- Len 按逻辑 row/offset count 更新，不能用 String payload bytes。

本文不承诺 ColumnStore/detached large chunk 能力。它们继续受 LegacyChunk32 gate 约束，超过边界时在 mutation/attempt 前显式拒绝。

---

## 十二、编译器发现不了的问题

依赖“把 Offset 类型改掉，再跟着编译错误修”只能发现签名不匹配，不能证明语义正确。以下项目必须人工审计和静态门禁。

### 12.1 显式 cast 和窄类型累计

以下代码仍可正常编译：

~~~go
uint32(len(col.Val))
uint32(len(encoded) - start)
uint32(writer.DataSize() - chunkStart)
offset += uint32(delta)
~~~

必须逐个确认它是 legacy boundary checked narrow，而不是把回绕重新放到新位置。

### 12.2 相同类型、不同语义

编译器无法区分：

- segment-local offset 与完整 ColVal offset。
- Segment.offset 文件物理位置与 String logical offset。
- logical row count 与 valid value count。
- memory Size 与 codec Size。
- uint32 wire 值与受控 low32；本文不允许后者。

### 12.3 Signedness

uint32 和 int32 都是 32 位，但上限不同。Arrow/tokenizer 的 MaxInt32 边界不会触发 Go 类型错误。

### 12.4 Endianness

把 TSSP、Record 和 Shelf 都接到同一个 byte helper 可以完全通过编译，却改变 wire。

### 12.5 unsafe、CGo 和 build tag

- unsafe.Pointer 几乎绕过全部 slice element type 检查。
- linux_amd64 fulltext 文件在 macOS 开发机不会被默认编译。
- CGo disabled 的 CI 不会覆盖 C ABI。
- 仅修改生成文件、不修改模板，会在下次 generate 时回退。

必须有 Linux amd64 + CGo 的编译/测试任务，并检查生成代码无 diff。

### 12.6 原地修改和部分提交

RemoveNilOffset 修改 source、append 一半后报错、decode error 前先 commit 都属于生命周期错误，不是类型错误。

### 12.7 String-only 分支

只判断 Field_Type_String 的分支会漏掉 Tag，但仍可编译。必须统一 IsVarLen 并用 Tag 测试矩阵覆盖。

### 12.8 资源计量

继续按 4-byte offset 计内存不会导致编译失败，只会使 admission 和内存指标低估。

---

## 十三、错误与原子性

建议错误分类：

| 错误 | 语义 | 基础版本处理 |
|---|---|---|
| ErrLegacyCodecOverflow | Record/Shelf/executor/Arrow/index 边界不可表示 | 可安全拆分则拆；否则返回 |
| ErrSegmentTooLarge | TSSP 单 segment 无法由 uint32 表示 | 多 row 可重新规划；单 row 返回永久错误 |
| ErrLargeChunkUnsupported | TSSP chunk 超过基础版本能力 | admission 应提前阻止；最终命中时 abort/告警 |
| ErrCorruptRecord | Record/bitmap/offset/frame 不自洽 | 拒绝输入 |
| ErrCorruptTSSP | String block/offset/length 不自洽 | 拒绝文件数据 |

统一要求：

- 不 panic。
- 不静默 cast。
- 不记录日志后继续发布。
- checked append 失败后目标不变。
- checked decode 失败后不暴露半构造对象。
- writer 边界失败后不发布 data/meta/index 的部分组合。

本文不展开 snapshot generation 的失败恢复，只要求基础 PR 不因为新增错误路径发布损坏文件。

---

## 十四、实施范围清单

### 14.1 record 核心

- lib/record/column.go
- lib/record/column_string.go
- lib/record/record.go
- lib/record/record_sort.go
- lib/record/column_sort.go
- lib/record/meger.go
- lib/record/record_check.go
- lib/record/schema.go
- lib/record/column_codec.go
- lib/record/record_codec.go
- lib/record/record_trans.go
- lib/record/iterator.go

同时检查 lib/binaryfilterfunc 等生成代码和模板。

### 14.2 TSSP / immutable

- lib/encoding/encoding.go
- lib/encoding/string.go
- engine/immutable/column_builder.go
- engine/immutable/chunkdata_builder*.go
- engine/immutable/reader.go
- engine/immutable/chunk_iterators.go
- engine/immutable/merge_util.go
- engine/immutable/detached_metadata.go
- engine/immutable/colstore/*

该清单只包含 ColVal、segment codec、checked size 和 LegacyChunk32 断言；不包含关联文档中的 ChunkMeta/query/snapshot large-chunk 改造。

### 14.3 Adapter

- engine/shard.go 中 Record/measurement wrapper。
- services/writer/decoder.go。
- coordinator/Record writer 路径。
- engine/shelf/wal_codec.go 及 Blob/Grouping/RecordMapper 调用链。
- services/consume/*。
- engine/iterator_plan.go。
- engine/executor/column.gen.go 及模板。
- engine/executor/chunk_codec.gen.go 及模板。
- engine/index/textindex/*。
- engine/index/sparseindex/bloom_filter*_index.go。
- engine/index/index.go 及 error 调用链。

### 14.4 静态审计

除编译外，使用 AST/type-based analyzer 或等价检查定位：

~~~text
ColVal.Offset 的赋值和参数传递
uint64/int64/len -> uint32/int32
unsafe.Pointer 与 offset slice
Bytes2Uint32Slice / Uint32Slice2byte
只判断 Field_Type_String 的可变长逻辑
以 len(Val)*4 或 len(Offset)*4 计内存
以 ChunkMeta.size 推导超过边界的物理 cursor
~~~

允许的窄化点必须带明确 boundary 名和 checked helper。

---

## 十五、测试方案

### 15.1 编译矩阵

- go test ./...。
- Linux amd64。
- CGo enabled 的 fulltext/index 构建和测试。
- 主要 build tags。
- go generate 后工作树无 diff。
- race 测试覆盖 Record/Arrow async 生命周期和共享 adapter。

### 15.2 ColVal 生命周期

- String 和 Tag 使用同一用例矩阵。
- null 在首、中、尾和 all-null。
- append 到空/非空目标。
- segment-local append rebase。
- split/slice 后首 offset 为 0。
- sort/merge/update/delete 后 offset 单调。
- signed length delta 增大和缩小。
- checked append 失败后 dst 完全不变。
- source Record 在 adapter 后完全不变。

### 15.3 边界算术

通过 helper/fake writer 测试，无需分配 4GiB 内存：

~~~text
MaxInt32-1 / MaxInt32 / MaxInt32+1
MaxUint32-1 / MaxUint32 / MaxUint32+1
base + local overflow
count * width overflow
frame overhead crossing boundary
V1 offsetCount * 4 crossing MaxUint32
~~~

### 15.4 Wire golden

- TSSP String V1/V2 新旧 fixture byte-for-byte。
- Record V1 little-endian fixture。
- measurement wrapper 1/255/256 字节和截断 name。
- Arrow/Record WAL 新旧读取。
- Shelf native/raw fixture。
- executor V1 fixture。
- consume/Kafka outer frame。

### 15.5 Decoder robustness

- 截断 header/payload/offset array。
- V1 offsetBytes 非 4 倍数。
- V2 rowCount==0。
- rowCount*4 超剩余 buffer。
- 非单调、越界、负 I32 offset。
- accumulated lengths 与 byteLen 不一致。
- Bitmap/NilCount/Len 不一致。
- trailing bytes。
- fuzz 要求不 panic、不超大分配。
- 32 位构建不得启用 write capability；所有 uint64 -> int helper 覆盖 MaxInt 和实际 buffer 双重边界。

### 15.6 Arrow

- 非零 Data.Offset 的 sliced String。
- N+1 sentinel，确认 ColVal 只保留 N 项。
- 非 byte-aligned validity bitmap 重对齐。
- with-null/no-null 一致。
- 输出补最终 sentinel。
- MaxInt32 payload 和 offset 边界。
- buffer alias/Retain 或 copy 生命周期。

### 15.7 executor 和 index

- executor null 列验证 valid-only offset count。
- transform append 跨 uint32 算术边界后仍为 uint64。
- executor V1 decode widen 和 malformed frame。
- tokenizer I32 与 fulltext U32 分别测试。
- local view rebase 和 C row range [0,n)。
- unsafe/CGo fixture 不把 uint64 内存直接交给 uint32_t*。
- index error 能传播到 builder。

### 15.8 LegacyChunk32 gate

- projected upper bound 在 MaxUint32 前允许。
- 请求跨界时在 mutation 前 rotate 或返回错误。
- 单请求超过空 generation 上限时无任何 mutation。
- writer actual segment/chunk size checked。
- chunk >MaxUint32 时不写 ChunkMeta、不发布文件。
- 基础版本测试中不存在 large chunk 成功用例。

### 15.9 内存和性能

- 短 String 场景 offset 翻倍后的 heap/GC。
- 大 String 场景吞吐和分配。
- Arrow widen copy 的 CPU/alloc。
- mutable admission 计量不再按 4-byte offset。

---

## 十六、独立 PR 与发布边界

### 16.1 PR 原子性

ColVal.Offset 是共享公开结构。以下内容必须在同一个可构建 PR 中完成：

1. checked helper 和 wire golden。
2. record 核心类型及生命周期。
3. TSSP pack/unpack。
4. Record/Shelf/Arrow/executor/index 等全部 adapter。
5. generated template 和 build-tag/CGo 路径。
6. 内存计量和 LegacyChunk32 gate。
7. 静态门禁和边界测试。

可以在 PR 内按 commit 分层审阅，但不能把仅修改 ColVal/encoding 的中间 commit 独立发布。

### 16.2 发布能力声明

该版本只声明：

~~~text
ColValOffsetU64
LegacyChunk32Writer
LegacyChunk32Reader
~~~

明确不声明：

~~~text
LargeChunkWriter
LargeChunkReader
LargeChunkCompact
LargeChunkSnapshot
~~~

### 16.3 空新集群发布

空新集群的发布步骤：

1. 固化所有 legacy wire golden。
2. 验证 startup/admission 能证明 generation encoded upper bound <= MaxUint32。
3. 部署完整 foundation binary，而不是混合包或局部补丁。
4. 保持 large chunk capability 关闭。
5. 监控 ErrLegacyCodecOverflow、ErrSegmentTooLarge、ErrLargeChunkUnsupported 和 admission rotate/reject。
6. 任何 ErrLargeChunkUnsupported 命中均视为 gate 漏洞，停止扩大流量。

### 16.4 Rolling 和回退

由于 wire 不变，普通 Record、WAL、TSSP、Shelf 和 executor frame 应支持新旧节点互通。

Go 源码/API 并不兼容：任何直接构造、接收或暴露 `ColVal.Offset []uint32` 的仓内及外部 Go 调用方都必须同步改为 `[]uint64` 并重新编译。这里的“兼容”仅指已版本化 wire/ABI 和正常范围持久化数据，不表示旧源码可与新结构混编。

回退前提：

- 从未生成 large chunk。
- 未改变任何持久化版本。
- 新版写出的普通 fixture 可被旧版读取。

如果出现 actual chunk >MaxUint32 的文件，说明 foundation 安全契约已被破坏，不能再依据本文声称可安全回退。

### 16.5 独立 PR 的完成定义

该 PR 完成的是：

- 消除 ColVal.Offset 自身的 uint32 累加和回绕。
- 将所有既有 32 位边界变成显式、可审计、可失败的 adapter。
- 为后续 large chunk 方案提供稳定的 uint64 内存基础。

该 PR 不完成的是：

- 让大于 MaxUint32 的单 SID chunk 成功落盘或可查询。
- 消除 LegacyChunk32 带来的 rotate/reject。
- 解决 large snapshot/compact 的工作集。

---

## 十七、向 large-chunk 设计交付的契约

`tssp-large-chunk-enablement-design.md` 可以依赖本文已经交付：

1. ColVal.Offset 和 executor 内存 offset 均为 uint64。
2. String/Tag 全生命周期不再存在 uint32 累加。
3. TSSP String segment codec 可 checked narrow/decode widen。
4. Record、Shelf、Arrow、executor、index adapter 均有明确 32 位边界。
5. 所有边界错误可传播，且 append/decode 不部分提交。
6. 内存计量按 8-byte offset。
7. 普通 wire golden 已固化。
8. LegacyChunk32 gate 和 ErrLargeChunkUnsupported 已存在且可观测。

large-chunk 文档独占以下设计责任：

- ChunkMeta 如何表示和恢复真实大长度。
- writer 物理 cursor 如何与 32 位 ChunkMeta.size 解耦。
- query 如何读取超过预读窗口的 entry。
- compact、merge、downsample、raw copy 的 large range。
- snapshot 如何避免完整 chunk buffer 和无界工作集。
- large writer/reader capability、发布顺序和回退边界。
- 何时以及在什么前置条件下移除 LegacyChunk32 gate。

在这些能力全部完成和发布前：

~~~text
Foundation ColValOffsetU64
    does not imply
LargeChunkWriter or LargeChunkReader
~~~

第二阶段只能显式替换或放宽 LegacyChunk32 契约，不能通过删除最终 checked cast 来“启用”large chunk。

---

## 十八、最终完成标准

以下条件全部满足，基础迁移才算完成：

1. 全仓 ColVal.Offset 为 []uint64。
2. String/Tag append、slice、sort、merge、update/delete 无 uint32 累加点。
3. ColVal 和 executor 两种 offset/null 数量语义分别验证。
4. TSSP/Record/Shelf/executor wire byte-for-byte 不变。
5. Arrow、tokenizer、fulltext 的 I32/U32 边界不混用。
6. 所有 narrow 可返回 error，且错误能到达任务/请求入口。
7. unsafe/CGo/build-tag/generated 路径全部覆盖。
8. Record.Size 按 8-byte memory offset，codec Size 保持 4-byte wire offset。
9. admission 能在 mutation 前证明 LegacyChunk32。
10. writer 对 segment/chunk 实际长度执行最终 checked assertion。
11. 基础版本不能成功产生 large chunk。
12. 文档和能力声明不暗示 query/snapshot/compact 已支持 large chunk。
