# Agent 任务交接总览

本目录面向接手本仓库的编码 Agent 与工程维护者，提供结构化的模块地图、当前状态、关键不变量、操作命令与交接清单。

- **[CURRENT-STATE.md](CURRENT-STATE.md)**：当前重构状态、核心不变量、已知坑点与禁动清单
- **[PLAYBOOK.md](PLAYBOOK.md)**：构建、测试、服务启停命令与交接检查模板

## 仓库结构与模块职责

| 目录 / 文件 | 职责说明 |
|---|---|
| `ingest/` | 原始 CSV 校验、格式解析、数据质量门禁 |
| `jobs/` | Java MapReduce 源码（`J01`–`J06`），由 `scripts/build-jobs.sh` 编译打包 |
| `orchestration/` | 作业调度、`resource_lease` 全局重任务锁、HDFS 封装、采样与空间保留（`retention.py`） |
| `ml/` | 分片数据集读取（`ShardDataset`）、`CONSTANT`/`LOGISTIC`/`GRU` 训练、验证选优与推理 |
| `publication/` | 分批装载（`release_load` 断点续传）、`STAGING -> READY -> RETIRED` 发布与回滚 |
| `service/` | FastAPI 只读接口（`/api/v1`）、游标签名与连接池管理 |
| `app/` | Streamlit 七页中文工作台（`app/main.py`）与自定义主题 |
| `migrations/` | MariaDB 表结构迁移 SQL（`001_control.sql` 等） |
| `config/` | 运行配置（`development.toml`）、Hadoop XML 配置与版本锁 |
| `scripts/` | 工具链安装、环境探针、服务启停（DB / Hadoop / Web）、验收与压测脚本 |
| `tests/` | 单元测试与隔离数据库集成测试（共享 fixture 位于 `tests/conftest.py`） |
| `var/` | 本地数据、数据库、HDFS、模型产物、日志与凭据（不入 Git） |

## 严格禁动清单（Do Not Touch）

运行或清理工作区时，**严禁修改、删除或覆盖**以下运行时资产：

1. `var/data/UserBehavior.csv`：约 3.42 GiB 原始数据集，重新获取耗时极长。
2. `var/presentations/`：演示与答辩产出目录。
3. `var/mariadb/`、`var/hdfs/`、`var/secrets/`：本地运行中或已初始化的数据库实例、HDFS NameNode/DataNode 存储及 `.cnf` 凭据。
4. `var/production/`：已发布版本归档与验收产物（除非明确执行重新发布或保留清理）。
