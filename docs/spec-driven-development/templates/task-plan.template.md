# Task Plan: <feature-name>

- Linked Spec: <path>
- Linked Design: <path>
- Iteration: <sprint/milestone>

## TP-1 任务分解

| Task ID | 描述 | 关联 Spec/Design | Owner | 预估 | 依赖 |
| --- | --- | --- | --- | --- | --- |
| T1 | <任务1> | FS-x, TD-x | <name> | <time> | - |
| T2 | <任务2> | FS-x, TD-x | <name> | <time> | T1 |
| T3 | <测试任务> | AT-x, TD-5 | <name> | <time> | T1/T2 |

## TP-2 实施顺序

1. <步骤 1>
2. <步骤 2>
3. <步骤 3>

## TP-3 Done 定义

- [ ] 代码实现完成，且与 Spec 条目一一映射。
- [ ] 测试命令可复现，结果可记录。
- [ ] 文档（配置/运维/迁移）已更新。
- [ ] 风险与回滚说明已验证。

## TP-4 AI 执行提示

```text
请按 T1 -> T2 -> T3 顺序执行；每完成一项输出：
1) 修改文件列表
2) 关联 Spec/Design 编号
3) 测试命令与结果
4) 未解决风险
```

