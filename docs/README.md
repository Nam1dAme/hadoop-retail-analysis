# 零售观察：设计与工程文档

本目录包含系统的架构设计、数据契约、接口规范与 Agent 交接文档（基线 `UBA-DOC-1.0.0`）。

## Agent 任务交接（新接手必读）

- **[handoff/README.md](handoff/README.md)**：仓库结构、模块边界与核心约束
- **[handoff/CURRENT-STATE.md](handoff/CURRENT-STATE.md)**：当前重构状态、关键不变量、已知坑点与禁动目录
- **[handoff/PLAYBOOK.md](handoff/PLAYBOOK.md)**：构建、测试、服务启停命令与交接检查清单

## 设计规范索引

| 文件 | 内容 |
|---|---|
| [00](00-governance.md) | 全局约定与版本规则 |
| [01](01-prd.md) | 需求、范围、指标及资源目标 |
| [02](02-ux.md) | 七页工作台、交互与空态 |
| [03](03-architecture.md) | 系统架构与组件职责 |
| [04](04-data-contracts.md) | 数据、时间、质量与产物契约 |
| [05](05-pipeline.md) | Hadoop 作业、样本构建、任务状态 |
| [06](06-database.md) | 逻辑表、字段、约束与索引 |
| [07](07-models.md) | 预测目标、GRU、训练评价与 CUDA |
| [08](08-api.md) | 只读 API 协议 |
| [09](09-environment.md) | 单机部署、环境锁与容量预算 |
| [10](10-operations.md) | 发布、恢复、安全及保留策略 |
| [11](11-acceptance.md) | 自动化与真实环境验收 |
| [12](12-agent-plan.md) | 模块职责与交付边界 |
| [13](13-decisions-risks.md) | ADR、迁移取舍及风险 |
| [14](14-traceability.md) | 需求—设计—任务—测试映射 |
| [15](15-evidence.md) | 数据来源与硬件参考边界 |
| [16](16-freeze-review.md) | 基线审查摘要 |
| [清单](BASELINE-MANIFEST.md) | 文件 SHA-256 清单 |
