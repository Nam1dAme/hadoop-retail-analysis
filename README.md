# Hadoop 用户行为分析与预测

当前已完成开发环境搭建，业务流水线、训练与七页工作台尚待实施。冻结设计见[文档入口](docs/README.md)。环境搭建记录见[执行报告](docs/execution/2026-11-02-environment.md)。

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
- API/UI预留8010/8510，尚未实现，没有服务监听。

```bash
scripts/hadoop-service.sh stop
python scripts/database-service.py stop
```

启动和停止脚本只管理本项目私有PID目录及数据库socket；不操作旧项目服务。YARN启用本项目文件状态存储，保留已完成应用记录；重启不保留正在运行的容器，重任务需由后续编排恢复。HDFS init仅允许空目录或已初始化目录，不重新格式化已有实例。依赖`setsid`使守护进程脱离启动终端；WSL退出/重启后需重新启动。当前不是系统自启动服务。

## 锁定与版本管理

`uv.lock`锁定全部Python依赖，`config/toolchain.lock.json`锁定Java/Hadoop发行包及官方摘要，`config/environment.lock.json`记录本机实际版本和配置hash。机器相关安装与数据都在忽略的`var`目录，凭据、CSV、模型、缓存和`.venv`不提交Git。

后续修改依赖用`uv lock`并重新验证；不要修改旧项目的虚拟环境或全局Java。HDFS为单机副本1，仅限本地研究。全量画像、业务数据库迁移、容量阶梯、完整训练与独立备份属于后续阶段，不能将本环境报告当作项目软件已验收。
