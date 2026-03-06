# openGemini Specification-Driven Development (SDD) 指南

> 目标：基于 openGemini 当前代码结构与工程实践，建立一套可直接驱动 AI 自动编码的规格先行（Specification-Driven Development）文档体系。

## 1. 适用范围

本目录适用于 openGemini 仓库内所有新增功能、重构、性能优化与跨模块改造，重点覆盖：

- `app/`：多进程入口与运行编排（`ts-meta`、`ts-store`、`ts-sql`、`ts-server`）。
- `engine/`：核心存储引擎、索引、执行器与查询规划。
- `coordinator/`：写入与查询协调逻辑。
- `services/`：保留、分层、写服务等控制面能力。
- `lib/`：通用基础库、配置、网络、观测与工具能力。
- `tests/`：集成测试入口。

## 2. 文档结构

- `01-repo-analysis.md`：面向 SDD 的仓库分析与模块边界。
- `02-sdd-lifecycle.md`：需求到上线的 SDD 生命周期与 AI 协作流程。
- `03-ai-coding-guardrails.md`：AI 自动编码约束、质量门禁和提示词约定。
- `templates/feature-spec.template.md`：功能规格模板（PRD + SRS 轻量合体）。
- `templates/technical-design.template.md`：技术设计模板（LLD）。
- `templates/task-plan.template.md`：任务分解与执行计划模板。
- `templates/acceptance-checklist.template.md`：验收与发布检查模板。

## 3. 推荐落地流程（最小可执行）

1. **先写 Feature Spec**：明确目标、范围、非目标、验收标准。
2. **补 Technical Design**：确认模块影响、数据流、接口、风险与回滚。
3. **生成 Task Plan**：拆解到可并行子任务与测试任务。
4. **AI 按任务编码**：严格引用 spec 段落编号，避免“自由发挥”。
5. **执行 Acceptance Checklist**：验证功能、性能、兼容性与可观测性。
6. **合并与沉淀**：把偏差、例外、事故复盘回写 spec。

## 4. 与业界实践对齐点

本套 SDD 体系参考了当前主流工程实践并做了 openGemini 化：

- **Spec-first**：先约束后实现，减少 AI 代码偏航。
- **Design traceability**：需求-设计-任务-测试全链路可追踪。
- **Shift-left quality**：在规格阶段显式声明测试、性能与回滚策略。
- **Prompt contract**：把 AI 提示词本身纳入工程资产管理。
- **Change budget**：对跨 `engine/coordinator/services` 改造强制风险预算。

## 5. 快速开始

以“新增一种写入限流策略”为例：

1. 复制 `templates/feature-spec.template.md` 到 `specs/<feature>/feature-spec.md`。
2. 复制 `templates/technical-design.template.md` 到 `specs/<feature>/technical-design.md`。
3. 复制 `templates/task-plan.template.md` 到 `specs/<feature>/task-plan.md`。
4. 复制 `templates/acceptance-checklist.template.md` 到 `specs/<feature>/acceptance.md`。
5. 将 `feature-spec` 与 `technical-design` 作为 AI 编码输入，按 `task-plan` 分步执行。

