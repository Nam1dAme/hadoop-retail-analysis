# Hadoop 用户行为分析与预测

项目已完成环境、全量处理、正式模型训练、版本发布、只读API及七页工作台。100,150,807行源数据已通过Hadoop处理，3,653,540条预测逐条核验通过。M7规模阶梯与恢复验收仍在进行。冻结设计见[文档入口](docs/README.md)，结果见[M1—M3验收](docs/execution/2026-11-02-m123-acceptance.md)与[M5—M6验收](docs/execution/2026-11-03-m5-m6-acceptance.md)。

工作台沿用旧项目的蓝白风格，真实七页桌面/窄屏与正式API检查通过。10并发持续五分钟，p95为0.193秒、错误率0；该成绩限于报告中的工作负载。当前执行状态见[后续开发记录](docs/execution/2026-11-02-m4-m7-development.md)。

当前源数据位于本项目`var/data/UserBehavior.csv`，是WSL本地文件，不是指向Windows挂载盘的符号链接。导入与环境锁脚本通过`config/development.toml`读取该路径。迁移前后的内容SHA256相同；历史基线、数据库来源记录与已发布产物清单中的旧路径记录当时的实际来源，不代表当前访问入口。数据文件位于Git忽略目录，不提交CSV。后续提交信息采用Conventional Commits：`type(scope): 中文描述`；type与scope保持英文，冒号后的描述使用中文，技术标识保留原样。

## 开发环境

本机使用 Python 3.12.3、PyTorch 2.6.0+cu124、Java 11、Hadoop 3.4.2、MariaDB 10.11。Java/Hadoop 位于本项目 `var/tools`，Python 位于 `.venv`。驱动、CUDA安装和MariaDB程序复用宿主；数据库数据、账号、HDFS数据及所有配置独立。

首次搭建（WSL终端执行；网络与设备访问在Codex中需获准沙箱外执行）：

```bash
python3 scripts/bootstrap-tools.py
# 可使用同一文件系统上的旧缓存实现硬链接，缓存路径不影响依赖锁。
UV_CACHE_DIR=/home/hadoop/hadoop-lab/var/cache/uv uv sync --locked --link-mode hardlink
source scripts/dev-env.sh
.venv/bin/python scripts/database-service.py init
scripts/hadoop-service.sh init
scripts/hadoop-service.sh start
```

已有安装日常启动：

```bash
source scripts/dev-env.sh
.venv/bin/python scripts/database-service.py start
scripts/hadoop-service.sh start
scripts/hadoop-service.sh status
```

环境验证：

```bash
source scripts/dev-env.sh
python scripts/check-environment.py
scripts/check-hadoop.sh
```

`check-environment.py`验证FP32/AMP CUDA更新、检查点及数据库最小权限；输出`var/environment-probe.json`。`check-hadoop.sh`编译Java11探针，向HDFS写入5条合成行为并提交真实YARN统计，预期pv=2、buy/cart/fav各1；证据在`var/probes/`。它不构成正式全量分析或模型训练验收。

## 连接与停止

- HDFS RPC：`hdfs://127.0.0.1:19000`；NameNode页面：<http://127.0.0.1:19870>。
- YARN页面：<http://127.0.0.1:18088>；客户端地址：`127.0.0.1:18032`。
- 数据库：`127.0.0.1:3307/behavior_analytics`。reader/writer/migrator凭据仅在`var/secrets/*.cnf`（权限0600）。管理入口为当前WSL用户的本项目Unix socket认证。
- 示例：`mariadb --defaults-extra-file="$PWD/var/secrets/reader.cnf" -e 'SELECT 1'`。
- API/UI使用8010/8510，均绑定localhost。未发布时返回明确空态，不展示随机数据。loader凭据同样保存在`var/secrets/loader.cnf`，与控制writer及API reader分离。

```bash
scripts/hadoop-service.sh stop
python scripts/database-service.py stop
```

启动和停止脚本只管理本项目私有PID目录及数据库socket；不操作旧项目服务。YARN启用本项目文件状态存储，保留已完成应用记录；重启不保留正在运行的容器，重任务需由后续编排恢复。HDFS init仅允许空目录或已初始化目录，不重新格式化已有实例。依赖`setsid`使守护进程脱离启动终端；WSL退出/重启后需重新启动。当前不是系统自启动服务。

## 锁定与版本管理

`uv.lock`锁定全部Python依赖，`config/toolchain.lock.json`锁定Java/Hadoop发行包及官方摘要，`config/environment.lock.json`记录本机实际版本和配置hash。机器相关安装与数据都在忽略的`var`目录，凭据、CSV、模型、缓存和`.venv`不提交Git。

后续修改依赖用`uv lock`并重新验证；不要修改旧项目的虚拟环境或全局Java。HDFS为单机副本1，仅限本地研究。M1—M3已完成全量画像、阶段数据库迁移与样本准备；容量阶梯、正式训练和独立备份仍须在后续阶段验收。

## M1—M3 数据链路

```bash
# 已生成的完整批次；status只读，不重新计算。
scripts/behaviorctl --baseline UBA-DOC-1.0.0 status --batch-id 1c577aba-46f2-4710-b296-5f29ac842921
# 新源的标准处理流程（同实现可复用成功产物；代码或参数变化产生新尝试）。
scripts/build-jobs.sh
scripts/behaviorctl --baseline UBA-DOC-1.0.0 --credentials var/secrets/migrator.cnf migrate
scripts/behaviorctl --baseline UBA-DOC-1.0.0 ingest --source-url https://tianchi.aliyun.com/dataset/649
scripts/behaviorctl --baseline UBA-DOC-1.0.0 pipeline --batch-id <UUID> --through J06
scripts/behaviorctl --baseline UBA-DOC-1.0.0 prepare-samples --batch-id <UUID>
```

具体契约见[导入](ingest/README.md)、[Hadoop作业](jobs/README.md)、[分片](ml/README.md)、[迁移](migrations/README.md)。完整执行入口为`python scripts/run-m123.py`，独立验收为`python scripts/verify-m123.py`。运行日志、质量报告和结果索引在`var/production/`，不会提交原始数据和分片。

当前样本索引为`var/production/samples.json`：TRAIN3463211、VALIDATION877575、TEST888982、FORECAST986594；总计6216362条、192个分片。未来窗口标签为-1，业务上表示尚无观测结果。M4可直接消费此已验证缓存及其manifest，无需为查看数据反复执行pipeline。

质量或任务失败返回非零退出码；查看对应`var/production/runs/<run_id>/driver.log`。`resume --batch-id <UUID> --through J06`从可校验结果继续；应用终态或原进程存活情况不明确时拒绝重复提交。`--dry-run`须放在子命令之前，只读检查依赖并打印计划。

## 后续阶段入口

```bash
source scripts/dev-env.sh
# 全量正式实验及同一检查点恢复；有重任务在运行时拒绝重复启动。
python scripts/train-models.py
# 模型实验成功后依次执行，不能省略验证和测试封存。
python scripts/setup-loader.py
python scripts/publish-results.py forecast
python scripts/publish-results.py publish
# 查询服务和页面；默认分别为 http://127.0.0.1:8010 与 :8510。
python scripts/web-service.py start
python scripts/web-service.py status
python scripts/web-service.py stop
```

工作台沿用旧项目实际Streamlit应用的蓝白配色与本地字体。页面不直接加载模型或读取HDFS；所有查询固定到一个完整发布版本。角色分离检查为`python tests/verify_permissions.py`。

M7工具包括`setup-scale.py`/`run-scale.py`、`load-test-api.py`、`backup-release.py`/`restore-backup.py`。规模测试使用独立库和HDFS根；记录抽样仅用于处理性能。备份与恢复均在本项目目录和隔离实例内进行。用户已确认没有另一块硬盘，因此本地副本及恢复演练不能满足独立介质备份门禁。
