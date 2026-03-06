# 01. 面向 SDD 的仓库分析（openGemini）

## 1. 代码基线概览

- 语言与模块：`go.mod` 定义模块 `github.com/openGemini/openGemini`，Go 版本 `1.24`。
- 仓库组织：采用“应用入口 + 核心引擎 + 协调层 + 基础库 + 服务层”分层。
- 应用形态：`app/` 下存在独立二进制入口（`ts-meta`、`ts-store`、`ts-sql`、`ts-server` 等）。

## 2. 核心架构分层（用于规格边界划分）

### 2.1 接入与进程编排层（`app/`）

- `app/ts-meta/main.go`：meta 节点入口。
- `app/ts-store/main.go`：store 节点入口。
- `app/ts-sql/main.go`：sql 节点入口。
- `app/ts-server/run/run.go`：本地一体化启动时将 SQL 写入与查询路径绑定到本地 store。

**SDD 建议**：涉及启动参数、节点角色、启动顺序的改动应归入“部署/运行规格”章节，而非仅写在实现备注中。

### 2.2 协调层（`coordinator/`）

负责写入/查询在逻辑层的组织与调度，连接上层 SQL 与下层引擎/存储。

**SDD 建议**：跨节点一致性、重试、幂等与背压策略需在 Feature Spec 明确“失败语义”。

### 2.3 引擎层（`engine/`）

- `immutable/` 与 `mutable/`：冷热与写入态数据管理。
- `index/`：多类索引实现。
- `executor/` + `hybridqp/` + `optimizer/`：查询执行与规划。

**SDD 建议**：任何 engine 变更必须在设计文档中给出：

1. 读写路径影响；
2. 内存与磁盘开销预算；
3. 与索引/查询计划的耦合点。

### 2.4 服务层（`services/`）

如保留策略、分层存储、写服务等可运维能力。

**SDD 建议**：服务层需求需附“可观测性指标”（日志、metric、trace）及“运维操作说明”。

### 2.5 基础库层（`lib/`）

提供配置、网络、编码压缩、缓存、观测、对象存储、并发工具等公共能力。

**SDD 建议**：`lib/` 改动默认视为“高扇出影响”，需要额外回归清单。

## 3. 工程与质量基线

- 构建：`python build.py`（见 `README.md` 与 `Makefile`）。
- 单测：`make gotest`。
- 集成测试：`make integration-test`。
- 静态检查：`make style-check`、`make go-vet-check`、`make static-check`。

**SDD 建议**：Task Plan 中必须至少包含：

- 单测（包级）；
- 集成/端到端（若涉及协议与跨模块）；
- 静态检查或等价质量门禁。

## 4. 适配 AI 自动编码的风险地图

1. **跨层隐式耦合风险**：`app` 初始化逻辑与 `engine/coordinator` 之间存在运行时绑定。
2. **性能回归风险**：时序数据库对写放大、查询延迟、压缩率敏感。
3. **兼容性风险**：对 InfluxDB 协议、InfluxQL、Prometheus/OTel 生态兼容要求高。
4. **配置风险**：配置项变更影响部署脚本与线上默认行为。

## 5. SDD 分层分规建议

- **L1（产品规格）**：用户场景、边界、成功指标。
- **L2（技术规格）**：模块变更、接口、数据模型、故障处理。
- **L3（执行规格）**：任务拆解、测试矩阵、发布与回滚。

推荐每个功能至少产出：`feature-spec` + `technical-design` + `task-plan` + `acceptance` 四件套。

