# 05. openGemini 代码架构深潜（流程 + 对象/接口）

> 本文面向“写规格的人”和“让 AI 按规格编码的人”，目标是回答两个问题：
> 1) 代码是如何跑起来的；2) 关键对象/接口分别负责什么。

## 1. 进程启动与编排总流程

## 1.1 多二进制启动模型

openGemini 采用多入口：`ts-meta`、`ts-store`、`ts-sql`、`ts-server`。

- `ts-meta`：元数据与集群管理。
- `ts-store`：存储与查询执行落地。
- `ts-sql`：SQL/HTTP 接入、写入协调、查询入口。
- `ts-server`：单机一体化模式，同时拉起 meta/store/sql 并进行本地 wiring。

## 1.2 启动控制平面

启动主线可抽象为：

```text
main.go -> app.Run(...) -> command.Run(...) -> NewServer(...) -> server.Open(...)
```

其中：

1. `app.Run` 负责命令解析、统一 run/version 分支与信号关闭流程。
2. `app.Command.Run` 负责配置加载、构造 Server、Open 生命周期管理。
3. 各子系统 `NewServer` 负责把配置转成运行对象。
4. 各 `Server.Open` 负责按顺序启动网络、元数据、存储、服务组件。

## 1.3 单机一体化（ts-server）特殊 wiring

`ts-server` 在 meta/store/sql 都启动后，执行本地注入：

- 将 SQL 写入落地存储切换为本地 `LocalStore`；
- 将 RPC Record 写入绑定本地存储；
- 将查询执行器的 local storage 指向本地 store。

这意味着在 SDD 中，**任何改动到 `run.InitStorage` 的行为都属于高风险 wiring 变更**。

## 2. 关键运行时对象（按层）

## 2.1 应用层（app）

- `app.Command`：统一命令对象，负责配置、Server 实例化、启动与关闭。
- `app.Server`（接口）：统一生命周期约束（`Open/Close/Err`）。
- `app.ServiceGroup`：批量管理服务组件（顺序 Open、统一 MustClose）。

**在 SDD 中的意义**：

- 任何新服务（监控、后台任务、消费者）都应声明是否纳入 `ServiceGroup`；
- 应明确 Open 顺序与 Close 顺序，避免资源泄漏。

## 2.2 元数据层（ts-meta + metaclient）

- `ts-meta/run.Server`：meta 节点的组合对象（监听、meta service、gossip、统计等）。
- `metaclient.Client`：各节点访问元数据面的客户端，负责获取节点/分片/路由信息。

**在 SDD 中的意义**：

- 任何依赖“元数据实时性”的功能（建表、分片、路由）都要声明一致性与重试策略。

## 2.3 接入/协调层（ts-sql + coordinator）

- `ingestserver.Server`：接入层容器，持有 `PointsWriter`、`QueryExecutor`、HTTP service 等。
- `coordinator.PointsWriter`：写入核心协调器，负责按元数据把行路由到分片/节点并处理重试。
- `coordinator.RecordWriter` / `services/writer.RecordWriter`：Record 路径写入协作对象。

**在 SDD 中的意义**：

- 写路径需求必须给出：失败语义（超时、重试、幂等）、限流策略、错误码策略。

## 2.4 存储/执行层（ts-store + engine）

- `ts-store/run.Server`：存储节点容器，负责 transport server、meta client、storage、stream 等生命周期。
- `engine.Engine`（接口）：存储引擎能力总接口，覆盖写入、DDL、查询计划、统计等高扇出能力。
- `engine.StorageService`（接口）：写入执行抽象（带节点 ID 与写回调）。

**在 SDD 中的意义**：

- 改 `Engine` 等核心接口属于 P0；需给出兼容性影响面和完整回滚路径。

## 2.5 服务扩展层（services）

- 典型服务：`continuousquery`、`hierarchical`、`writer`、`arrowflight`、`sherlock` 等。
- 多数服务以接口方式解耦核心层（例如 writer service 的 PointWriter/Authorizer）。

**在 SDD 中的意义**：

- 服务需求需声明其依赖接口，不应直接侵入 engine 内部实现细节。

## 3. 关键接口地图（AI 编码重点）

| 接口/对象 | 所在模块 | 作用 | SDD 注意点 |
| --- | --- | --- | --- |
| `app.Server` | `app` | 统一进程生命周期契约 | Open/Close 顺序必须在设计中声明 |
| `app.Service` | `app` | 子服务统一抽象 | 新增后台任务须纳入关闭路径 |
| `PWMetaClient` | `coordinator` | 写入路径元数据能力集合 | 字段/分片变更会直接影响写路由 |
| `PointsWriter` | `coordinator` | 写入协调与重试 | 必须定义 timeout/重试/错误语义 |
| `Engine` | `engine` | 存储引擎总能力接口 | P0 变更，要求兼容性 + 性能回归 |
| `StorageService` | `engine` | 写入执行抽象 | 影响写路径一致性和吞吐 |
| `RecordWriter` | `services/writer` | Record 写入封装 | 与 SQL/Store wiring 耦合紧密 |

## 4. 两条主链路：写入与查询

## 4.1 写入链路（逻辑视角）

```text
Client -> ts-sql(http/rpc) -> coordinator.PointsWriter
      -> MetaClient(路由/分片信息) -> TSDBStore(local or net)
      -> ts-store/storage -> engine(mutable/immutable/index)
```

设计文档必须回答：

1. 写入失败如何处理（重试/丢弃/返回错误）；
2. 是否改变路由策略或分片选择；
3. 是否引入新配置，默认值是否兼容。

## 4.2 查询链路（逻辑视角）

```text
Client Query -> ts-sql QueryExecutor -> planner/executor
             -> ts-store(engine logical plan/scan/index)
             -> 聚合/拼装 -> response
```

设计文档必须回答：

1. 逻辑计划或执行计划是否变化；
2. 索引依赖与过滤下推是否变化；
3. 对 P95/P99 的预算与回归验证方法。

## 5. 架构改动的 SDD 写法建议

当需求涉及“整体代码流程/接口职责”时，Technical Design 建议固定增加：

- `TD-x.1 现状流程图`（改动前）；
- `TD-x.2 目标流程图`（改动后）；
- `TD-x.3 接口影响清单`（新增/修改/删除）；
- `TD-x.4 兼容性与回滚`（特别是默认值、协议、数据格式）。

这样 AI 执行时就不会只改“局部函数”，而是能被约束在完整架构语义内。


## 6. 源码定位索引（便于 AI/评审快速跳转）

- 启动与命令框架：
  - `app/command.go`
  - `app/server.go`
- 单机编排与本地 wiring：
  - `app/ts-server/main.go`
  - `app/ts-server/run/run.go`
- 元数据节点：
  - `app/ts-meta/run/cmd.go`
  - `app/ts-meta/run/server.go`
- 存储节点：
  - `app/ts-store/run/server.go`
- SQL 接入与协调：
  - `app/ts-sql/sql/server.go`
  - `coordinator/points_writer.go`
- 核心引擎接口：
  - `engine/engine_interface.go`

建议：在 Technical Design 的“文件清单”里至少覆盖以上一个入口文件 + 一个核心接口文件，避免只改实现不改契约说明。

