# openGemini 查询执行引擎 spec

本文档从 `engine/executor/pipeline_executor.go` 出发，记录查询执行引擎的实现级约束和运行语义。设计导览见 `query_engine_design.md`。

## 1. 范围

本文档覆盖：

- `Select` 到 `PipelineExecutor` 的主要构建链路。
- `ExecutorBuilder` 如何把 logical plan 转成 transform DAG。
- `PipelineExecutor` 的并发、取消、错误、abort、release 语义。
- `Processor`、`ChunkPort`、`Chunk` 的运行时契约。
- exchange、reader、RPC、limit 等关键边界。

不覆盖：

- 每个 aggregate 函数的数学语义。
- 存储 cursor 的底层文件读取细节。
- SQL parser 和 shard mapper 的完整实现。

## 2. 查询构建链路

### 2.1 Select

入口：

```go
func Select(ctx context.Context, stmt *influxql.SelectStatement, shardMapper query.ShardMapper, opt query.SelectOptions) (hybridqp.Executor, error)
```

行为：

1. 调用 `query.Prepare(stmt, shardMapper, opt)` 得到 prepared statement。
2. 从 context 中读取 `query.QueryDurationKey`，记录 prepare 耗时。
3. 检查子查询排序方向约束。
4. 调用 `preparedStatement.Select(ctx)`。
5. 返回 `hybridqp.Executor`，不在此处执行。

约束：

- context 中必须存在 `*statistics.SQLSlowQueryStatistics`，否则返回错误。
- prepared statement 必须 defer close。

### 2.2 preparedStatement.BuildLogicalPlan

输入：

- `SelectStatement`
- `ProcessorOptions`
- `LogicalPlanCreator`
- 当前时间 `now`

输出：

- best logical plan
- 可选 trait 信息，例如 ts-server local store 场景的 `MultiMstReqs`

主要步骤：

1. 空 fields 直接返回 nil。
2. 将 `now` 写入 context。
3. 设置 `opt.EnableBinaryTreeMerge`。
4. 重写字段 alias。
5. 构造 `QuerySchema`。
6. 根据 plan type 尝试套用 `SqlPlanTemplate`。
7. 否则调用 `buildExtendedPlan`。
8. 打印 origin plan。
9. ts-server local store 场景可移除 node exchange。
10. 使用 heuristic planner 查找 best plan。
11. column store 场景可能改写为 hash agg/hash merge。

### 2.3 buildExtendedPlan

职责：

1. 调用 `buildQueryPlan` 构造 source 到 query operator 的主体逻辑计划。
2. 如果 `CanQueryPushDown()`，调用 `BuildNodeExchange` 插入 node exchange。
3. 如有 `select into` target，追加 `LogicalTarget`。
4. 最后追加 `LogicalHttpSender` 或 hint sender。

### 2.4 ExecutorBuilder.Build

入口：

```go
func (builder *ExecutorBuilder) Build(node hybridqp.QueryNode) (hybridqp.Executor, error)
```

行为：

1. nil node 返回 nil。
2. clone logical plan root。
3. 调用 `addNodeToDag` 递归构造 `TransformDag`。
4. 调用 `buildAnalyze` 建 tracing span。
5. 返回 `NewPipelineExecutorFromDag(builder.dag, builder.root)`。

注意：

- logical plan 会 clone，避免构建过程中的 trait 注入影响原 plan。
- `NewPipelineExecutorFromDag` 会立即连接 DAG 中的 ports 并生成 processor 列表。

## 3. TransformDag 构建规则

### 3.1 addNodeToDag dispatch

`ExecutorBuilder.addNodeToDag` 按节点类型分派：

| logical node | 构建路径 |
| --- | --- |
| `LogicalExchange` | `addExchangeToDag` |
| `LogicalReader` | `addReaderToDag` |
| `LogicalIndexScan` | `addIndexScan` |
| `LogicalSparseIndexScan` | `addSparseIndexScan` |
| `LogicalHashMerge` | 旧计划走 `addHashMerge`，unify plan 按 exchange/default |
| `LogicalHashAgg` | 旧计划走 `addHashAgg`，unify plan 按 exchange/default |
| `LogicalColumnStoreReader` | `addColStoreReader` |
| other | `addDefaultNode` |

### 3.2 addDefaultNode

`addDefaultNode` 的递归规则：

1. 遍历当前 logical node 的 children。
2. clone child，并继承当前 node 的 trait。
3. 递归 `addNodeToDag(child)`。
4. 多 measurement plan node 在 child 间调用 `NextMst()`。
5. 尝试 `CanOptimizeExchange` 消除单 shard / 单 reader exchange。
6. 调用 `addDefaultToDag(node)` 创建当前 transform。
7. 将所有 child vertex 连到当前 vertex。

### 3.3 addDefaultToDag

`addDefaultToDag` 负责普通 logical node 到 processor 的转换：

1. dummy node 返回 nil。
2. 如果是 `LogicalReader`，从 `StoreExchangeTraits` 取 reader cursor。
3. 通过 `GetTransformFactoryInstance().Find(node.String())` 找到 creator。
4. 调用 `creator.Create(logicalPlan, processorOptions)` 创建 processor。
5. 创建 `TransformVertex` 并加入 DAG。
6. 对特殊 processor 注入 DAG/vertex：
   - `LimitTransform`
   - `HttpSenderTransform`
   - `HttpSenderHintTransform`

失败条件：

- 没有 matching transform creator。
- creator 创建 processor 返回错误。
- `LogicalReader` 没有可用 reader。

### 3.4 Transform creator registry

注册接口：

```go
type TransformCreator interface {
    Create(LogicalPlan, *query.ProcessorOptions) (Processor, error)
}
```

注册方式：

```go
var _ = RegistryTransformCreator(&LogicalLimit{}, &LimitTransformCreator{})
```

查找 key 是 `LogicalPlan.String()`。

Reader 类有独立的 `ReaderCreatorFactory`：

```go
type ReaderCreator interface {
    CreateReader(plan hybridqp.QueryNode, frags interface{}) (Processor, error)
}
```

## 4. Exchange 构建规则

### 4.1 Exchange 接口

`Exchange` 扩展 `hybridqp.QueryNode`：

- `Schema()`
- `EType()`
- `ERole()`
- `ETraits()`
- `AddTrait()`
- `ToProducer()`

### 4.2 exchange type

`addExchangeToDag` 按 `EType()` 分派：

| ExchangeType | 行为 |
| --- | --- |
| `NODE_EXCHANGE` | producer 生成 RPC sender；consumer 生成 RPC reader + merge。 |
| `PARTITION_EXCHANGE` | column store partition traits，随后 default exchange。 |
| `SHARD_EXCHANGE` | 按 shard clone child，单 shard 可优化。 |
| `SINGLE_SHARD_EXCHANGE` | 当前 shard trait，随后 default/binary merge。 |
| `READER_EXCHANGE` | 按 reader clone child，单 reader 可优化。 |
| `SERIES_EXCHANGE` | series 维度汇聚，可使用 binary tree merge。 |

### 4.3 producer role

`addNodeProducer`：

1. 要求 exchange 只有一个 child。
2. 设置当前 `IndexScanExtraInfo` 的 shard 或 pt query。
3. 递归构建 child。
4. 需要有效 `spdy.Responser`。
5. 创建 `RPCSenderTransform`。
6. 添加 child -> sender edge。

### 4.4 consumer role

`addNodeConsumer`：

1. 要求有 traits 且只有一个 child。
2. 每个 `RemoteQuery` trait 创建一个 `RPCReaderTransform`。
3. clone exchange，转 producer 后下发给 RPC reader。
4. 根据 exchange 类型创建 merge/hash agg/hash merge transform。
5. 添加 reader -> merge edges。

### 4.5 default exchange

`addDefaultExchange`：

1. 要求 traits 非空、child 数为 1。
2. 对每个 trait clone child 并 `ApplyTrait(trait)`。
3. 递归构建每个 child。
4. 创建 exchange processor：
   - `LogicalExchange`：`MergeTransform` 或 `SortedMergeTransform`
   - `LogicalHashAgg`：`HashAggTransform`
   - `LogicalHashMerge`：`HashMergeTransform`
5. 将所有 child 连到 merge processor。

### 4.6 binary tree exchange

`addBinaryTreeExchange(exchange, nLeaf)` 用递归二叉树替代单个多输入 merge。

语义：

- `nLeaf < 2` 时直接构建 leaf child。
- 否则拆成左右子树。
- 每层创建一个 merge processor。

目的：

- 降低单个 merge transform 的输入 fan-in。
- 改善大量 shard / reader 汇聚时的调度和内存压力。

## 5. Port 与 DAG 连接

### 5.1 ChunkPort

字段：

- `RowDataType`
- `State chan Chunk`
- `OrigiState chan Chunk`
- `Redirected bool`
- `once *sync.Once`

连接语义：

- `Connect(to)` 创建 `make(chan Chunk, PORT_CHAN_SIZE)`。
- `PORT_CHAN_SIZE == 1`。
- 输出端和输入端共享同一个 channel。
- `Equal(to)` 要求 row data type 相等。

关闭语义：

- `Close()` 使用 `sync.Once`，可重复调用。
- redirected port close 前恢复原始 state。
- close nil channel 会被跳过。

### 5.2 TransformDag

`TransformDag` 保存：

- `mapVertexToInfo`
- `edgeSet`

`AddEdge(from, to)` 同时维护：

- `fromInfo.directEdges`
- `toInfo.backwardEdges`

`PipelineExecutor.init()` 用 backward edges 连接 ports：

```go
Connect(edge.from.transform.GetOutputs()[0], edge.to.transform.GetInputs()[i])
```

约束：

- 当前实现默认每条 DAG edge 使用 from 的第 0 个 output。
- to input 使用 backward edge 在 `info.backwardEdges` 中的位置。
- 多输出 processor 需要确保 builder 或 transform 逻辑与此约束一致。

### 5.3 DAG 辅助类型

`dag.go` 中还有一个 `DAG` 类型，主要用于 explain、测试和从已连接 processors 反推出图。

它通过 port channel 的 connection id 建立边：

- input connection id -> processor
- output connection id -> processor

与 `TransformDag` 的关系：

- `TransformDag` 是 `ExecutorBuilder` 和 `PipelineExecutor` 的主运行时 DAG。
- `DAG` 是通用图描述和校验辅助。

## 6. PipelineExecutor 运行语义

### 6.1 构造

`NewPipelineExecutor(processors)`：

- 直接使用调用方已连接好的 processors。
- 常用于单元测试。

`NewPipelineExecutorFromDag(dag, root)`：

- 保存 dag/root。
- 调用 `init()` 连接端口并收集 processors。

### 6.2 ExecuteExecutor

`ExecuteExecutor(ctx)`：

1. 增加 `ExecutorStat.ExecScheduled`。
2. 调用 `Execute(ctx)`。

### 6.3 InitContext

`InitContext(ctx)`：

1. 加锁。
2. 若 executor 已有 context 或 cancelFunc，返回 `PipelineExecuting`。
3. 创建 `context.WithCancel(ctx)`。
4. 保存 context 和 cancel function。

约束：

- 一个 `PipelineExecutor` 同一时间只允许一次执行。
- `Execute` 结束后必须 `destroyContext()`。

### 6.4 Execute

流程：

1. runtime timer begin。
2. `InitContext(ctx)`。
3. defer `Release()`。
4. defer `destroyContext()`。
5. 为每个 processor 启动一个 goroutine。
6. goroutine 调用 `exec.work(processor)`。
7. 第一个返回 error 的 processor 触发 crash/no-mark-crash。
8. 每个 processor 结束后调用 `FinishSpan()`。
9. 等待所有 processor。
10. 若 `processorErr == NoFieldSelected`，返回 nil。
11. 否则返回第一个 processor error。

### 6.5 work

`exec.work(processor)`：

- defer recover panic。
- panic 时记录 stack，并调用 `exec.Crash()`。
- 调用 `processor.Work(exec.context)`。
- 非 nil error 会记录日志并返回给 `Execute`。

### 6.6 Crash

`Crash()`：

1. recover 自身 panic。
2. 加锁。
3. 如果已经 crashed，返回。
4. 设置 `crashed = true`。
5. 调用 `cancel()`。
6. 调用 `processors.Interrupt()`。
7. 调用 `processors.Close()`。

`processors.Interrupt()` 受 `sysconfig.GetInterruptQuery()` 控制。

### 6.7 NoMarkCrash

`NoMarkCrash()`：

1. 设置 `crashed = true`。
2. cancel context。
3. `processors.InterruptWithoutMark()`。
4. `processors.Interrupt()`。
5. `processors.Close()`。

用于 `errno.IsRetryErrorForPtView` 返回 true 的错误。

### 6.8 Abort

`Abort()`：

1. 加锁。
2. 若已 aborted，返回。
3. 设置 `aborted = true`。
4. 调用 `closeSinkTransform()`。
5. 增加 `ExecutorStat.ExecAbort`。

`closeSinkTransform()`：

- 仅当 dag/root 非 nil 时生效。
- 新 goroutine 从 root 沿 backward edge DFS。
- 对 `IsSink() == true` 的 transform 调用 `Abort()`。

用途：

- limit、sender 等下游节点已经得到足够数据时，提前停止上游 reader/RPC reader。

### 6.9 Release

`Release()`：

- 遍历所有 processors。
- 调用 `p.Release()`。
- 记录 release 错误但不返回。

约束：

- processor 的 `Release` 必须容忍 Work 已退出、Close 已调用或 Abort 已调用。

## 7. Processor 契约

### 7.1 Work

`Work(ctx)` 必须满足：

- 监听输入 channel close。
- 监听 `ctx.Done()`。
- 正常结束时关闭自己的输出 ports。
- 出错时返回 error，由 executor 统一中断。

source processor：

- 没有 input。
- 从存储、RPC 或测试数据读取 chunk。
- 输出完毕后关闭 output。

transform processor：

- 有 input 和 output。
- input close 后 flush 缓存并关闭 output。

sink processor：

- 有 input，无 output。
- input close 或 ctx done 后退出。

### 7.2 Close

`Close()` 通常关闭 output ports，或对 reader/RPC 发送中止信号。

注意：

- 下游依赖上游 output close 退出。
- crash 会调用所有 processors 的 `Close()`，因此 Close 必须尽量幂等。

### 7.3 Abort / Interrupt

`Abort()`：

- 用于主动停止 reader/RPC reader。
- 不一定表示 error。

`Interrupt()`：

- 用于 crash/cancel 时通知远端或底层资源。

`InterruptWithoutMark()`：

- 用于可重试错误，避免远端把中断标记为普通 crash。

### 7.4 tracing

构建阶段：

- `ExecutorBuilder.Analyze(span)` 设置 root tracing span。
- `buildAnalyze()` 从 root 沿 backward edge 给每个 processor 创建 span。

运行阶段：

- processor 在 `Work` 中用 `StartSpan` 创建局部 span。
- `PipelineExecutor` 在 goroutine 结束时调用 `FinishSpan()`。

## 8. Chunk 规格

`Chunk` 由多个接口组成：

- `ChunkMeta`
- `ChunkTag`
- `ChunkTime`
- `ChunkColumn`
- `ChunkOperator`
- `ChunkSerialization`
- `ChunkGraph`

`ChunkImpl` 字段：

| 字段 | 语义 |
| --- | --- |
| `rowDataType` | 列 schema。 |
| `name` | measurement。 |
| `tags` | series tags。 |
| `tagIndex` | 每个 tag 对应的起始 row index。 |
| `time` | 时间列。 |
| `intervalIndex` | group by time 窗口起始 row index。 |
| `columns` | field columns。 |
| `dims` | dimension columns。 |
| `Record` | 底层 record。 |
| `graph` | graph query payload。 |

约束：

- `len(time)` 应与每个 column 的行数一致。
- `tagIndex`、`intervalIndex` 必须指向合法 row index。
- processor 改写行数时必须同步更新 time、tagIndex、intervalIndex。
- `ChunkBuilder` 应按 `RowDataType` 初始化正确列类型。

## 9. 典型 processor 行为

### 9.1 ChunkReader

位置：`engine/iterator_plan.go`

职责：

- 从 cursor 读取 `record.Record`。
- 转换为 `executor.Chunk`。
- 生成 interval index。
- 写入 output port。

结束条件：

- `closed` signal。
- `ctx.Done()`。
- cursor 无数据。
- 读取错误。

`ChunkReader.IsSink() == true`，因此可被 `PipelineExecutor.Abort()` 主动中止。

### 9.2 FilterTransform

位置：`filter_transform.go`

职责：

- 从 input 读取 chunk。
- 根据 condition 过滤行。
- 维护 tag / interval 信息。
- 输出过滤后的 chunk。

特点：

- 内部使用 `currChunk` channel 和一个辅助 goroutine 解耦 input 读取和过滤。
- input close 后 flush 当前 result chunk，并关闭 output。

### 9.3 LimitTransform

位置：`limit_transform.go`

职责：

- 根据 limit/offset 和 limit type 裁剪 chunk。
- 可保存 DAG/vertex。
- 满足 limit 后可沿 DAG abort 上游 sink。

### 9.4 RPCReaderTransform

位置：`rpc_transform.go`

职责：

- 将 distributed query node marshal 后发给远端。
- 通过 `RPCClient` 接收 chunk response。
- 每个 response 转为 `Chunk` 写 output port。

关闭/中断：

- `Abort()` 调用 client abort。
- `Interrupt()` 调用 client interrupt。
- `Close()` abort 并关闭 output。

### 9.5 RPCSenderTransform

位置：`rpc_transform.go`

职责：

- 从 input port 读取 chunk。
- 编码为 `ChunkResponse`。
- 通过 `spdy.Responser` 写回远端调用方。

## 10. 错误与关闭不变量

- 任一 processor 返回非 nil error，executor 必须取消 context 并中断/关闭所有 processors。
- panic 必须被 recover，并转为 crash。
- output port close 是下游正常退出的主要信号。
- `ctx.Done()` 是 crash、外部取消、超时的统一退出信号。
- `Abort()` 不应把 executor 标记为 crashed。
- `NoFieldSelected` 被视为非错误结果，`Execute` 返回 nil。
- retry error for pt view 使用 `NoMarkCrash()`。
- `Release()` 在 `Execute` defer 中执行，即使 `InitContext` 后续失败也会进入 defer 路径。

## 11. 扩展新 transform 的要求

新增一个 logical node + transform 时应满足：

1. 实现 `LogicalPlan`，提供稳定的 `String()`。
2. 实现 `Processor`，通常嵌入 `BaseProcessor`。
3. 定义 input/output `ChunkPort`，其 `RowDataType` 必须与逻辑计划匹配。
4. 实现 `GetInputs`、`GetOutputs`、`GetInputNumber`、`GetOutputNumber`。
5. 在 `Work(ctx)` 中监听 input close 和 `ctx.Done()`。
6. 正常退出时关闭 output port。
7. 注册 creator：
   ```go
   var _ = RegistryTransformCreator(&LogicalX{}, &XTransformCreator{})
   ```
8. 为关键行为添加 pipeline 单元测试。

## 12. 关键测试映射

| 测试文件 | 覆盖点 |
| --- | --- |
| `pipeline_executor_test.go` | executor 并发、crash、abort、exchange builder、context 复用。 |
| `dag_test.go` | DAG 构建、creator registry、边关系。 |
| `executor_test.go` | `NewPipelineExecutorFromDag`、builder 构图和运行。 |
| `filter_transform_test.go` | filter processor 行为。 |
| `limit_transform_test.go` | limit / offset / abort 相关行为。 |
| `merge_transform_test.go` | 多输入 merge transform。 |
| `agg_transform_test.go` | stream aggregate 和 chunk 流转。 |
| `mock_tsdb_system_test.go` | 接近完整查询路径的 mock 系统测试。 |

## 13. 参考代码

- `engine/executor/select.go`：查询入口、logical plan 构建和优化。
- `engine/executor/logic_plan.go`：logical plan 类型和 builder。
- `engine/executor/pipeline_executor.go`：executor builder、transform DAG、pipeline executor。
- `engine/executor/processor.go`：port、processor、connect、processor collection。
- `engine/executor/chunk.go`：chunk 数据模型。
- `engine/executor/dag.go`：creator registry 和辅助 DAG。
- `engine/executor/filter_transform.go`：典型单输入单输出 transform。
- `engine/executor/limit_transform.go`：可触发 abort 的 transform。
- `engine/executor/rpc_transform.go`：分布式查询 reader/sender。
- `engine/iterator_plan.go`：存储 reader 到 chunk 的 source processor。
