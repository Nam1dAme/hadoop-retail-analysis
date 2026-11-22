# 工程操作手册与交接清单

## 常用构建与测试命令

```bash
# 1. 加载项目环境变量（Java 11 / Hadoop 3.4.2 / .venv）
source scripts/dev-env.sh

# 2. 编译与测试 Java MapReduce 作业
scripts/build-jobs.sh
scripts/test-jobs.sh

# 3. 运行 Python 单元测试（不依赖外部服务）
.venv/bin/pytest
# 若在独立 worktree 中调用主仓库虚拟环境：
PYTHONPATH=. /home/hadoop/hadoop-retail-analysis/.venv/bin/pytest

# 4. 运行包含 MariaDB 的集成测试（需先启动 MariaDB 并初始化测试库）
python scripts/setup-test-database.py
BEHAVIOR_INTEGRATION=1 .venv/bin/pytest
```

## 服务启停与运维命令

```bash
source scripts/dev-env.sh

# MariaDB 私有实例（端口与地址读取 config/development.toml，默认 127.0.0.1:3307）
python scripts/database-service.py start
python scripts/database-service.py status
python scripts/database-service.py stop

# Hadoop 伪分布式服务（HDFS + YARN + HistoryServer）
scripts/hadoop-service.sh start
scripts/hadoop-service.sh status
scripts/hadoop-service.sh stop

# Web 服务（FastAPI + Streamlit）
python scripts/web-service.py start
python scripts/web-service.py status
python scripts/web-service.py stop

# 空间保留与清理演练
python scripts/retention-plan.py
```

## 脚本索引

`scripts/` 中的脚本分为四类。验收类脚本没有被其他代码直接调用，但它们产出的 `var/production/*-verification.json` 会被 `audit-delivery.py` 与 `publication/backup.py` 读取，因此不是死代码，不要删除。

| 类别 | 脚本 | 用途 |
|---|---|---|
| 环境 | `dev-env.sh`、`bootstrap-tools.py`、`check-environment.py`、`freeze-environment.py`、`check-hadoop.sh`、`check-host-storage.ps1` | 加载环境、安装锁定工具、环境探针与冻结 |
| 服务 | `database-service.py`、`hadoop-service.sh`、`web-service.py`、`setup-loader.py`、`setup-test-database.py` | 私有 MariaDB/Hadoop/Web 启停与账号权限 |
| 生产流程 | `behaviorctl`、`build-jobs.sh`、`test-jobs.sh`、`train-models.py`、`publish-results.py`、`backup-release.py`、`restore-backup.py`、`retention-plan.py` | 摄取→流水线→训练→发布→备份/恢复，保留计划只读 |
| 发布后验收 | `verify-models.py`、`verify-release.py`、`verify-api.py`、`verify-entity-ranking.py`、`verify-model-page.py`、`verify-browser.py`、`verify-ui-interactions.py`、`load-test-api.py`、`monitor-resources.py`、`audit-delivery.py` | 逐条核对模型/预测/API/排行/前端，最后由 `audit-delivery.py` 汇总证据 |
| 规模基准 | `setup-scale.py`、`run-scale.py` | 1M/10M/全量 YARN 规模测试，隔离于生产 |
| 资源 | `subset-ui-fonts.py` | 重建前端字体子集 |

## 提交规范（源自 AGENTS.md）

- 使用 Conventional Commits：`type(scope): 中文描述`（例如 `fix(scripts): 修复环境变量未加载时的 Python 路径回退`）。
- 每个 commit 只包含一个逻辑变更；严禁将原始数据集、`.cnf` 凭据、缓存或 `var/` 产物提交到 Git。

## Agent 交接记录模板

完成一轮任务后，在回复或 PR/交接说明中按以下精简格式汇报即可，无需编写冗长日记：

```markdown
### 交接摘要
- **分支 / Commit**：`<branch>` / `<git log --oneline -n 5>`
- **已完成**：
  - [ ] 改动点 1（附关键文件路径）
  - [ ] 改动点 2
- **验证结果**：
  - 命令：`<实际执行的测试或检查命令>`
  - 结果：`<通过/跳过/失败数，注明是否含集成测试>`
- **未决事项 / 阻塞**：<无，或具体阻塞点与复现方式>
```
