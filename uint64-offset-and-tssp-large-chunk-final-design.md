# TSStore ColVal uint64 与 TSSP Large Chunk 最终设计

> 文档状态：评审稿。本文整合 ColVal 64 位内存偏移与 TSSP large chunk 两项设计，仅描述目标方案、设计约束和能力边界，不包含实施步骤与代码修改清单。

本文只定义 `EngineType=TSSTORE` 的 ColVal/TSSP large-chunk 数据链。其他系统不在设计范围内，本文不对其行为与兼容性作结论。

## 一、方案阐述

### 1.1 设计目标

面向大字符串、多 Series 瞬时高吞吐写入场景，消除 String/Tag 数据在内存累计过程中的 32 位 offset 回绕风险，并支持 TSStore 单个 TSSP chunk 超过 4 GiB。

方案同时满足以下目标：

- 不改变 TSStore TSSP、Record 和 WAL 的既有格式；
- 不以 shard 级 4 GiB 限制作为常规写入背压，避免大流量场景出现显著性能退化；
- large chunk 的写入、读取及下游消费均保持有界工作集；
- 普通数据继续保持新旧版本双向兼容。

### 1.2 总体方案

方案分为两个递进层次：

1. **建立 64 位内存基础语义**
   - ColVal 的 String/Tag 逻辑 offset 统一使用 64 位表达；
   - ChunkMeta 的内存 size 统一使用 64 位表达，并始终表示 chunk 的真实物理长度；
   - 所有内存计算保持 64 位语义，进入既有 32 位格式边界时再执行受检转换。

2. **开放 TSStore TSSP large chunk 能力**
   - TSSP 磁盘格式保持不变，Segment size 继续使用 32 位；
   - 磁盘 ChunkMeta size 继续保持现有 32 位值域和编码，普通 chunk 写入真实值，large chunk 写入真实长度的低 32 位；
   - TSStore reader 根据完整的列与 Segment 物理元数据恢复真实 chunk 长度，磁盘 size 字段不再作为运行时长度依据；
   - writer 根据实际物理写入范围确定 chunk 长度及后续位置，不依赖被截断的磁盘 size；
   - 小 chunk 保留整块处理能力，large chunk 采用 Segment 级流式写入、读取和消费。

### 1.3 能力范围

| 范围 | 设计结论 |
| --- | --- |
| ColVal String/Tag | 支持超过 32 位范围的进程内累计 offset |
| TSStore TSSP chunk | 支持真实长度超过 4 GiB |
| 单个 TSSP Segment | 继续受现有 32 位格式上限约束 |
| TSStore Record/WAL 边界 | 格式不变，按现有边界受检转换 |
| 运行架构 | 持久化写角色仅支持 64 位架构 |
| 历史损坏文件 | 仅恢复 size 回绕且绝对 offset 正确的文件，不自动修复已污染的绝对 offset |

本设计不扩展单 value、单 Segment 或 TSStore Record/WAL frame 的既有上限，也不改变 TSSP 的文件版本、字段宽度和字段顺序。

TSStore WAL replay 工作集优化和 merge streamMode 的 large chunk 流式能力属于独立工作项。在相应能力完成前，可能进入这些路径的 large chunk 写入必须保持关闭。

### 1.4 核心原则

- **内存语义唯一**：ColVal offset 表示逻辑位置，ChunkMeta size 表示真实物理长度，不维护并行的截断长度语义。
- **线格式稳定**：64 位能力只扩展内存表达和运行时语义，不隐式升级外部格式。
- **边界显式**：除磁盘 ChunkMeta size 的兼容低 32 位语义外，任何 64 位到 32 位转换都必须确认目标格式可表达。
- **工作集有界**：large chunk 不触发整块多 GiB 内存分配。
- **失败原子**：越界、损坏或能力不足时，不暴露半构造结果，不发布部分文件。
- **Reader First**：所有读取角色具备识别、恢复或拒绝 large chunk 的能力后，才允许开启 large writer。

## 二、详细设计

### 2.1 数据模型与统一语义

| 对象 | 内存语义 | 持久化语义 |
| --- | --- | --- |
| ColVal offset | 64 位逻辑字节位置 | 按目标 codec 的既有宽度编码 |
| ChunkMeta offset | chunk 的 64 位物理起点 | 保持现有格式 |
| ChunkMeta size | chunk 的 64 位真实物理长度 | 保留现有 32 位值域与编码 |
| Segment offset | Segment 的 64 位物理起点 | 保持现有格式 |
| Segment size | Segment 的真实编码长度，且可由 32 位表达 | 保持现有 32 位字段 |

其中：

- **Legacy32 chunk** 指真实长度不超过 32 位上限的 chunk；
- **large chunk** 指真实长度超过 32 位上限、但由多个合法 Segment 组成的 TSStore TSSP chunk；
- 磁盘 ChunkMeta size 字段逐步退化为兼容字段。TSStore 运行时真实 size 一律由物理元数据恢复并覆盖。

完成解码后，所有上层读写与消费逻辑只依赖统一的 64 位 ChunkMeta size，不再感知磁盘字段是否发生回绕。

### 2.2 ColVal 64 位内存基础

String/Tag 的 offset 应满足以下不变量：

- offset 数量与逻辑行数一致，null 行同样保留逻辑 offset；
- offset 单调不减，且不得超过 value buffer 的实际长度；
- 最后一行的结束位置由 value buffer 长度确定；
- 列数据被组合或重组时，局部 offset 必须按目标 value buffer 重新基准化；
- 非变长列不持有 offset；
- ColVal 逻辑 offset 与 TSSP 物理文件 offset 始终是两类独立语义。

64 位 offset 必须贯穿 TSStore 写入、查询及 TSSP 编解码所经过的 ColVal 生命周期。任何中间 32 位累计都会重新引入回绕风险，因此不能形成混合宽度的内存状态。

内存容量评估按 8 字节 offset 计算；编码大小评估仍按目标格式的实际宽度计算。64 位逻辑值转换为内存索引前，仍需受实际 buffer 长度及平台索引范围约束。

### 2.3 既有 32 位边界

TSStore TSSP Segment、Record 和 WAL 保持原有布局、版本、字节序和数值语义。

跨边界遵循以下规则：

- 从 32 位格式读入内存时无损扩展为 64 位；
- 写入 32 位格式前，先确认单个目标对象可由该格式完整表达；
- signed 32 位和 unsigned 32 位边界分别判断，不得混用；
- 不通过内存重解释绕过宽度、对齐或 ABI 校验；
- 无法表达的数据必须在产生外部结果前显式失败，不能截断或回绕。

磁盘 ChunkMeta size 是唯一的特例：在 TSStore large chunk 中，它按现有 32 位语义保存真实 size 的低 32 位，并仅用于兼容和一致性校验，不承担真实长度语义。

### 2.4 TSStore 真实 chunk 范围

TSStore TSSP 文件打开后，ChunkMeta 解码必须基于完整的列与 Segment 物理元数据恢复 chunk 的真实起点、终点和长度，不能由单次查询、选列结果或磁盘 size 字段临时决定。

恢复过程遵循以下约束：

- 全部列和 Segment 的物理区间都参与范围恢复；未选列可以不物化，但不能从范围计算中省略；
- 恢复结果不依赖列元数据的排列顺序；
- Segment 区间及布局开销共同构成完整 chunk 范围；
- 各物理区间必须连续、无重叠、无越界，并落在文件数据区内；
- 恢复出的真实 size 低 32 位必须与磁盘 ChunkMeta size 一致。

任一条件不成立时，该 chunk 按损坏数据处理，不使用磁盘 size 猜测或拼接结果。

### 2.5 写入语义

写入侧以实际产生的物理字节范围作为唯一位置依据：

- 列长度、chunk 长度和物理位置均使用足够宽的类型累计，不经过 32 位中间值；
- chunk size 由实际写入起点和终点确定，后续 chunk 从真实终点继续；
- 每个 Segment 必须独立满足现有 32 位编码上限；
- large chunk 由多个合法 Segment 或列累计形成，不放宽单 Segment 格式边界；
- large chunk 以 Segment 为单位形成有界写入工作集，文件轮转发生在 chunk 边界；
- 写入失败时不发布不完整的 chunk 元数据或文件结果。

磁盘 ChunkMeta size 对普通 chunk 保持原值，对 large chunk 保留真实 size 的低 32 位。该字段的写入不影响运行时物理游标和后续数据定位。

### 2.6 读取、查询与下游消费

TSStore reader 对外提供的 ChunkMeta 必须已经完成元数据归一化，其 size 始终是真实长度。上层消费者不再直接解释磁盘 size。

查询按 chunk 能力分流：

- Legacy32 chunk 可继续使用整块预读和现有快速路径；
- large chunk 依据 Segment 的真实 offset/size 定位读取，不按完整 chunk 大小分配内存；
- 任一读取范围同时受 chunk 边界和文件数据区边界约束；
- 解码异常不能产生部分可见的查询结果。

TSStore compact、merge、snapshot、复制等整块消费者遵循统一能力契约：具备 large chunk 能力的路径采用 Segment 级流式或固定缓冲处理；不具备该能力的路径必须在读取、分配或输出前明确拒绝，不能退化为截断的 32 位处理。

TSStore merge streamMode 在独立优化完成前属于不具备 large chunk 能力的路径，不参与 large writer 的可达范围。

### 2.7 资源控制与异常语义

64 位表示解决的是格式表达上限，不代表放弃资源治理。资源控制应基于实际内存、I/O 和并发预算，而不是以 shard 级 4 GiB 格式上限统一拒绝写入。

large chunk 路径必须满足：

- 峰值工作集由单 Segment、固定缓冲及必要元数据约束，不随完整 chunk 大小线性增长；
- 资源不足、格式不可表达、能力未开放和数据损坏是不同的异常语义；
- 所有长度加法、范围计算、内存下标转换和外部格式转换均受检；
- 异常以显式错误结束，不 panic、不静默继续、不产生部分输出。

若历史文件仅发生磁盘 size 回绕且物理区间有效，可恢复真实 size 并记录可观测事件；若绝对 offset 污染、区间重叠或越界，则告警并隔离文件。

### 2.8 兼容性与能力演进

| 场景 | 兼容结论 |
| --- | --- |
| 旧版本普通 TSSP 文件由新 reader 读取 | 兼容 |
| 新版本普通 TSSP 文件由旧 reader 读取 | 兼容，线格式不变 |
| 新版本 large TSSP 文件由新 reader 读取 | 兼容 |
| 新版本 large TSSP 文件由旧 reader 读取 | 不兼容，必须通过能力门禁隔离 |

由于 TSSP 不新增磁盘格式版本，large chunk 能力必须通过节点、角色和 TSStore 文件读写能力共同约束：

1. **基础与 reader 阶段**：完成 64 位内存语义、TSStore 真实 size 恢复、查询及下游消费者的支持或显式拒绝；large writer 保持关闭。
2. **writer 阶段**：确认所有可能读取目标文件的角色均已升级，并且必要的有界处理能力就绪后，对 TSStore 开放 large writer。

关闭 large writer 只能阻止新增 large 文件，不能消除已有文件对新 reader 的依赖。回退到旧 reader 前，必须确保其不会接管 large 文件，或先将已有 large 文件重写为 Legacy32 可表达的形态。

历史文件是否存在绝对 offset 污染，不能依靠单次随机查询证明。启用 large writer 前应完成文件级顺序审计；只对 size 回绕且绝对范围正确的文件进行自动恢复，其余文件进入告警和隔离流程。

### 2.9 设计成立条件

本设计通过评审与后续验收时，应至少满足以下条件：

- 普通数据的磁盘及 wire 字节保持兼容；
- ColVal 和 ChunkMeta 的运行时计算不存在 32 位回绕路径；
- 单 Segment 始终满足既有格式上限，large chunk 可由多个合法 Segment 正确组成；
- TSStore reader 能稳定恢复真实 size，并拒绝不连续、重叠或越界的物理范围；
- large chunk 的写入、查询和已声明支持的消费者保持有界工作集；
- 未声明支持的 TSStore 消费路径在产生副作用前失败；
- reader-first 门禁能够阻止旧 reader 接管 large 文件；
- 历史可恢复异常与物理 offset 损坏能够被明确区分。
