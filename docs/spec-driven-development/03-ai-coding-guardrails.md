# 03. AI 自动编码 Guardrails（openGemini）

## 1. 编码边界约束

1. **不越层修改**：需求仅影响 `services/` 时，禁止无依据改动 `engine/`。
2. **不隐式改默认值**：配置默认值变更必须进入 Spec Change Proposal。
3. **不破坏兼容协议**：涉及 Influx Line Protocol / InfluxQL / Prometheus / OTel 的变更必须列兼容性验证。
4. **不跳过测试映射**：每个实现任务都要有可执行验证命令。

## 2. 规格到代码映射规范

提交说明中建议使用：

- `Spec:` `FS-<id>`（Feature Spec 条目）
- `Design:` `TD-<id>`（Technical Design 条目）
- `Test:` `AT-<id>`（Acceptance Test 条目）

示例：

```text
Spec: FS-3, FS-7
Design: TD-2.1, TD-4.3
Test: AT-1, AT-4
```

## 3. 提示词模板（用于 AI 自动编码）

```text
[Context]
Repo: openGemini
Target: <feature-name>

[Spec]
- Use specs/<feature>/feature-spec.md sections: <...>
- Use specs/<feature>/technical-design.md sections: <...>
- Only implement tasks listed in specs/<feature>/task-plan.md: <...>

[Constraints]
- Keep compatibility with existing external protocols.
- No hidden behavior changes.
- Add/update tests mapped to acceptance IDs.

[Output]
1) Brief plan
2) Code changes grouped by file
3) Test commands and expected outcomes
4) Risks/rollback notes
```

## 4. 测试矩阵最小集

- 功能正确性：正向 + 逆向 + 边界。
- 兼容性：协议/接口不回退。
- 稳定性：并发、重试、超时、失败恢复。
- 性能（关键路径改动时）：吞吐、P95/P99、资源占用。

## 5. 变更级别分级

- **P0（高风险）**：`engine/` 读写路径、索引、执行器。
- **P1（中风险）**：`coordinator/`、`services/`、跨模块配置。
- **P2（低风险）**：`lib/` 局部工具、文档、测试辅助。

建议策略：

- P0：强制双人评审 + 性能回归报告。
- P1：至少一名模块 Owner 评审。
- P2：常规评审。

