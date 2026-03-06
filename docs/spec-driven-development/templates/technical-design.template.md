# Technical Design: <feature-name>

- Linked Feature Spec: <path>
- Owner: <team/person>
- Reviewers: <owners>
- Risk Level: P0 | P1 | P2

## TD-1 设计摘要

<一句话结论 + 关键取舍>

## TD-2 影响范围与文件清单

- 目录影响：
  - [ ] app/
  - [ ] coordinator/
  - [ ] engine/
  - [ ] services/
  - [ ] lib/
  - [ ] tests/
- 文件清单：
  - `<file-path>`: <改动说明>

## TD-3 架构与关键流程

### TD-3.1 控制流
<请求进入 -> 协调 -> 引擎/服务 -> 返回>

### TD-3.2 数据流
<关键结构、编码、持久化与读取链路>

### TD-3.3 并发与一致性
<锁、队列、重试、幂等、失败语义>

## TD-4 接口与配置

- 接口变更：<API/RPC/内部接口>
- 配置项：
  - Name: <key>
  - Type: <type>
  - Default: <value>
  - Reloadable: yes/no
  - Compatibility: <说明>

## TD-5 测试设计

- Unit Tests：<新增/修改包与用例>
- Integration Tests：<场景与数据准备>
- Perf Smoke：<指标、基线、对比方式>
- 验收映射：`AT-x -> test-case`

## TD-6 风险、回滚与发布策略

- 风险清单：<技术风险>
- 回滚策略：<特性开关/配置回退/版本回退>
- 发布策略：<灰度范围与观察窗口>

## TD-7 备选方案对比

| 方案 | 优点 | 缺点 | 结论 |
| --- | --- | --- | --- |
| A |  |  |  |
| B |  |  |  |

## TD-8 运维与可观测性

- 新增 metrics：<name + label>
- 关键日志：<日志字段>
- 告警建议：<阈值与告警规则>

