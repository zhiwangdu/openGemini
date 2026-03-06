# 03. AI 自动编码 Guardrails（openGemini）

## 1. 强约束（Must）

1. 不得越层改动（例如需求只在 `services/`，不得无依据触碰 `engine/`）。
2. 不得隐式改变默认配置值。
3. 不得修改外部协议行为而不声明兼容验证。
4. 每个任务必须给出可执行测试命令。
5. 每个提交必须有 `FS/TD/AT` 映射。

## 2. 推荐约束（Should）

- 小步提交：每个提交聚焦单一任务。
- 可回滚优先：先加开关再替换旧路径。
- 观测先行：新功能同时提供日志/metrics 证据。

## 3. 提交与评审映射规范

建议在 commit message 或 PR 描述中使用：

```text
Spec: FS-3.1, FS-5
Design: TD-2, TD-4.2
Test: AT-1, AT-3
Risk: P1
Rollback: disable <feature-flag>
```

## 4. AI Prompt Contract（复制即用）

```text
[Context]
Repo: openGemini
Feature: <name>
Risk Level: P0/P1/P2

[Spec Inputs]
- feature-spec: <sections>
- technical-design: <sections>
- task-plan: <task IDs>

[Rules]
- Implement only listed tasks.
- Keep protocol/config compatibility.
- Map every code change to FS/TD items.
- Provide tests mapped to AT items.

[Output Format]
1) Plan (with IDs)
2) File-by-file changes
3) Test commands + expected result
4) Rollback notes
```

## 5. 测试矩阵（最低要求）

| 维度 | 必测项 | 适用条件 |
| --- | --- | --- |
| 功能正确性 | 正向、反向、边界 | 全部 |
| 协议兼容 | Influx/Prom/OTel 行为不回退 | 涉及接口或协议 |
| 稳定性 | 并发、超时、重试、恢复 | 跨模块或分布式路径 |
| 性能 | 吞吐、P95/P99、资源占用 | P0 或关键路径 |

## 6. 常见反模式（禁止）

- “顺手重构”导致超范围改动。
- 没有 Spec 编号映射的代码提交。
- 没有验收证据就宣称完成。
- 先改代码，后补规格（倒置流程）。

