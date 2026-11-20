# 当前项目状态与核心不变量

## 当前状态

- **工程主线**：M0–M7 主流程已完整实现，覆盖数据导入、Hadoop `J01`–`J06` MapReduce、`CONSTANT`/`RULE`/`GBDT`/`LOGISTIC`/`GRU` 评测与 CUDA 加速、原子发布与回滚、FastAPI 只读服务、Streamlit 七页中文工作台。
- **当前活跃开发分支**：`fix/ml-evaluation-v2`（样本 v2 双日测试集 + 20 维特征 + `RULE`/`GBDT` + `J01` map-only 优化 + `011` 双日主键与校验修复），详细进度见 [HANDOFF-2026-11-20.md](HANDOFF-2026-11-20.md)。
- **近期清理与重构**：
  - `scripts/` 已移除一次性里程碑/交接脚本，统一从 `config/development.toml` 读取端口与连接参数（经 `scripts/_common.py` 提供默认回退），去除了硬编码本机路径、GPU 型号及系统发行版断言，并合并了重复的 MariaDB `.cnf` 生成逻辑。
  - `tests/` 已将共享 `api` fixture 提取到 `tests/conftest.py`（解决跨文件导入导致重复建表插数的问题，并兼容空 `data_batch` 表），移除了依赖旧版固定 UUID 的过期脚本。
  - `docs/` 已清理临时执行日志与过度防御性样板话术，建立 `docs/handoff/` 交接文档体系。

## 核心不变量（修改代码时必须遵守）

1. **基线与版本标识**：
   - `baseline_version = "UBA-DOC-1.0.0"`、`policy_version = "behavior-v1"`、`split_version = "daily-48h-24h-v2"`。
   - 任何涉及 `orchestration/`、`ingest/`、`ml/` 的代码改动都会改变 `implementation_fingerprint()`，正式产物必须记录真实 `git rev-parse HEAD`。
2. **三重隔离约束（Three-Way Isolation）**：
   - 合成测试或压测配置（`synthetic = true` 或 `benchmark = true`）**必须**同时隔离数据库名（非 `behavior_analytics`）、HDFS 根目录（非 `/behavior/v1`）与本地 `work_dir`（非 `var/production`），否则 `orchestration.common.load()` 会拒绝加载。
3. **时间与 ID 语义**：
   - 审计时间戳统一使用 UTC ISO8601（带 `Z` 后缀）；业务日期统一为北京时间（`Asia/Shanghai`），范围遵循左闭右开 `[start, end)`。
   - `user_id`、`item_id`、`category_id` 统一作为十进制字符串处理，排序为 UTF-8 字节序。
4. **模型评价与发布闭环**：
   - 阈值选择与模型优胜仅依据 `VALIDATION` 集，`TEST` 集封存评价，`FORECAST` 集无观测标签且严禁回流重训。
   - 发布切换通过 `current_release` 表 `singleton_id=1` 原子 CAS 更新完成。

## 环境前提与已知坑点

1. **文件权限与 ACL（`hadoop:hadoop` vs `nam1dame`）**：
   - 主仓库 `/home/hadoop/hadoop-retail-analysis` 及其 `.venv/`、`var/` 目录由 `hadoop:hadoop` 拥有；当以 `nam1dame` 或其他受限用户身份在 worktree 中运行测试时，可能需要设置 `PYTHONPATH=.` 并显式调用主仓库的 `.venv/bin/pytest`，且注意不要破坏 `var/secrets/` 的 `0700`/`0600` 权限。
2. **Hadoop / Java 环境变量**：
   - 执行任何 Hadoop 命令或 `scripts/build-jobs.sh`、`scripts/hadoop-service.sh` 前，必须先 `source scripts/dev-env.sh`，确保 `BEHAVIOR_ROOT`、`JAVA_HOME`、`HADOOP_HOME`、`HADOOP_CONF_DIR` 指向本项目的 `var/tools/` 与 `config/hadoop/`。
3. **集成测试开关**：
   - 默认运行 `pytest` 会跳过依赖 MariaDB 的 10 个集成测试；需要执行全量集成测试时，先运行 `python scripts/setup-test-database.py`，再设置 `BEHAVIOR_INTEGRATION=1`。
4. **单重任务锁**：
   - `resource_lease`（`singleton_id=1`）保证同一时间只有一个重任务（pipeline/训练/发布）持有租约，切勿在训练或批量导入期间并发启动另一个重任务。
