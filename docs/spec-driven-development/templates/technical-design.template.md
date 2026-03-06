# Technical Design: <feature-name>

- Linked Feature Spec: <path>
- Owner: <team/person>
- Reviewers: <owners>

## TD-1 设计摘要

<一段话总结方案>

## TD-2 影响范围

- 目录影响：
  - [ ] app/
  - [ ] coordinator/
  - [ ] engine/
  - [ ] services/
  - [ ] lib/
  - [ ] tests/
- 文件清单：
  - `<file-path>`: <改动说明>

## TD-3 架构与数据流

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
- Integration Tests：<场景>
- Perf Smoke：<指标与基线>
- 验收映射：`AT-x -> test-case`

## TD-6 风险、回滚与发布策略

- 风险清单：<技术风险>
- 回滚策略：<开关/配置回退/版本回退>
- 发布策略：<灰度范围与观察窗口>

## TD-7 备选方案对比

| 方案 | 优点 | 缺点 | 结论 |
| --- | --- | --- | --- |
| A |  |  |  |
| B |  |  |  |

