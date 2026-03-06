# openGemini Specification-Driven Development (SDD) 指南

> 目标：把“需求 -> 设计 -> 编码 -> 验证 -> 发布”全过程规格化，使 AI 自动编码在 openGemini 中可控、可追踪、可回滚。

## 1. 为什么需要 SDD（面向 openGemini）

openGemini 是分布式时序数据库，核心路径（写入、索引、查询执行）跨 `app/coordinator/engine/services/lib` 多层协作，存在天然复杂性：

- 跨层耦合：入口初始化与引擎能力绑定（例如本地一体化启动场景）。
- 性能敏感：吞吐、P95/P99、压缩率、内存占用高度敏感。
- 兼容性约束：对 InfluxDB v1 协议、InfluxQL、Prometheus/OTel 生态兼容要求高。

SDD 的价值是：**先约束后实现**，让 AI 的改动严格落在规格边界内，减少“看似可用但不可运营”的代码。

## 2. 文档结构

- `01-repo-analysis.md`：仓库分层、模块边界、风险地图、变更分级。
- `02-sdd-lifecycle.md`：从需求到发布复盘的标准生命周期。
- `03-ai-coding-guardrails.md`：AI 编码边界、提示词契约、质量门禁。
- `04-example-write-throttle.md`：端到端填写样例（从规格到任务与提示词）。
- `05-architecture-deep-dive.md`：代码架构深潜（整体流程、关键对象与接口职责）。
- `templates/`：四件套模板（Feature Spec / Technical Design / Task Plan / Acceptance）。

## 3. 使用方式（推荐）

1. 从 `templates/feature-spec.template.md` 起草需求规格。
2. 用 `templates/technical-design.template.md` 明确模块影响、接口与风险。
3. 用 `templates/task-plan.template.md` 拆解为可交付任务（每项必须有测试）。
4. 用 `templates/acceptance-checklist.template.md` 验收并留痕。
5. 通过 PR 将 `specs/<feature>/` 与代码一并提交，确保审查可追溯。

## 4. 目录约定（建议执行）

```text
specs/
  <feature-name>/
    feature-spec.md
    technical-design.md
    task-plan.md
    acceptance.md
    change-log.md           # 可选：记录 Spec Change Proposal
```

## 5. 快速开始

以“新增写入限流策略”为例：

1. 创建 `specs/write-throttle-v2/`。
2. 填写四件套文档。
3. 先评审 `feature-spec + technical-design`，后开始编码。
4. 编码时在每个提交说明里标注 `FS-x / TD-x / AT-x` 映射。
5. 合并前跑最小质量门禁并完成 `acceptance.md`。

## 6. 评审最小标准

- 规格是否定义了 **In Scope / Out of Scope**。
- 是否有明确失败语义（超时、重试、幂等、降级、回滚）。
- 是否列出配置项变更与默认值兼容策略。
- 是否声明关键指标与观测策略（日志、metrics、trace）。

