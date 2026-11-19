# 12 模块边界与分阶段交付

基线 `UBA-DOC-1.0.0`。

## 阶段计划

| 阶段 | 工作与依赖 | 退出条件 |
|---|---|---|
| M0 环境与数据画像 | 全量 profile、GPU/内存/磁盘核验、依赖锁与独立配置 | `check-environment.py` 与 Hadoop 探针通过 |
| M1 工程与数据 | 控制库、来源登记、HDFS 归档、`J01`/`J02`、质量门禁 | 幂等与故障单测通过 |
| M2 Hadoop 分析 | `J03`/`J04`、分析表、区间 UV/复访 | 全量真实 YARN 与守恒校验通过 |
| M3 时间样本 | `J05`/`J06`、分片导出、normalization、split 封存 | 人工样例与全量样本数、正例率一致 |
| M4 模型与 CUDA | `CONSTANT`/`LOGISTIC`/`GRU` 训练、验证选择、TEST 封存 | 保存设备证据、模型与资源报告 |
| M5 预测与发布 | `FORECAST`、结果装载、原子 release、恢复演练 | 发布内容完整、回滚不混版 |
| M6 服务与页面 | FastAPI 全部只读接口、Streamlit 七页工作台与权限校验 | 真实 release 贯通、无演示假数据 |
| M7 规模与交付 | 100 万/1000 万/全量阶梯、性能优化、交接文档与验收报告 | 全部测试与压测门禁通过 |

## 目录职责

- `ingest/`：来源校验、解析与质量门禁
- `jobs/`：Java MapReduce 作业源码与打包
- `orchestration/`：调度、租约控制、恢复、HDFS 封装与保留清理
- `ml/`：分片读取、特征构造、模型训练、评价与推理
- `service/`：FastAPI 只读接口与游标签名
- `app/`：Streamlit 七页中文工作台
- `migrations/`：MariaDB 表结构迁移 SQL
- `config/`：非敏感配置、Hadoop XML 与版本锁
- `tests/`：单元测试与隔离数据库集成测试
- `scripts/`：环境检查、服务启停、验收与压测脚本
- `var/`：本地数据、数据库、HDFS、模型与凭据（不入 Git）
- `docs/handoff/`：Agent 任务交接与运行状态文档

## 阶段交接要求

交接记录应包含：完成事项、改动文件/提交、测试命令与实际结果、资源峰值、遗留问题，以及禁止修改的目录清单。详细模板见 [docs/handoff/PLAYBOOK.md](handoff/PLAYBOOK.md)。
