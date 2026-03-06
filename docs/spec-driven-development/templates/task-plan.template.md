# Task Plan: <feature-name>

- Linked Spec: <path>
- Linked Design: <path>
- Iteration: <sprint/milestone>

## TP-1 任务分解

| Task ID | 描述 | 关联 Spec/Design | 输出物 | Owner | 预估 | 依赖 |
| --- | --- | --- | --- | --- | --- | --- |
| T1 | <实现任务 1> | FS-x, TD-x | code | <name> | <time> | - |
| T2 | <实现任务 2> | FS-x, TD-x | code | <name> | <time> | T1 |
| T3 | <测试任务> | AT-x, TD-5 | tests/report | <name> | <time> | T1/T2 |
| T4 | <文档/发布任务> | FS-x, TD-6 | docs/runbook | <name> | <time> | T3 |

## TP-2 实施顺序

1. <步骤 1>
2. <步骤 2>
3. <步骤 3>

## TP-3 Done 定义

- [ ] 代码实现完成，且与 Spec 条目一一映射。
- [ ] 测试命令可复现，结果可记录。
- [ ] 文档（配置/运维/迁移）已更新。
- [ ] 风险与回滚说明已验证。

## TP-4 风险跟踪

| 风险 | 触发条件 | 应对策略 | Owner |
| --- | --- | --- | --- |
| <risk-1> | <condition> | <mitigation> | <name> |

## TP-5 AI 执行提示

```text
按 T1 -> T2 -> T3 -> T4 顺序执行；每完成一项输出：
1) 修改文件列表
2) 关联 FS/TD/AT 编号
3) 测试命令与结果
4) 剩余风险
```

