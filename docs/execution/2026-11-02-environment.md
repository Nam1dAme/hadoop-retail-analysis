# 2026-11-02 环境搭建与验证记录

引用冻结基线 UBA-DOC-1.0.0。此次完成环境配置与本地Git版本控制，不等于M0全量画像或软件业务验收完成。没有远程推送。

## 已实装与隔离

| 组件 | 实际版本/路径 | 验证 |
|---|---|---|
| Python | 3.12.3，项目`.venv` | uv.lock离线同步成功，78个依赖包 |
| PyTorch | 2.6.0+cu124，CUDA runtime12.4 | 单卡可见，FP32/AMP反向更新、checkpoint往返通过 |
| GPU | RTX4060 Laptop，8187.5MiB显存 | 实际torch设备属性；驱动及瞬时占用见environment.lock |
| Java | Temurin11.0.32.1，var/tools/java11 | 官方SHA256校验、javac --release11编译通过 |
| Hadoop | 3.4.2，var/tools/hadoop-3.4.2 | 官方SHA512校验；真实HDFS/YARN任务成功 |
| Maven | 3.8.7 | source新环境后使用Java11；探针直接javac/jar构建 |
| MariaDB | 10.11.14 | 私有实例127.0.0.1:3307；buffer pool256MiB、max connections30 |
| Web依赖 | FastAPI/Streamlit/Plotly及完整依赖锁 | 导入与FastAPI TestClient请求成功；业务API/UI尚未实现 |

HDFS RPC19000、Web19870，YARN客户端18032、Web18088；绑定localhost。独立HDFS数据在var/hdfs，YARN在var/yarn，数据库在var/mariadb；全部位于本项目。旧Hadoop/Java、旧数据库3306及旧项目代码均未替换。

未取得现有MariaDB管理权限，因而复用系统MariaDB程序运行项目私有实例，没有修改sudo策略或旧账号。新reader/writer/migrator密码由安全随机生成，仅写权限0600的var/secrets，不入Git；实例管理使用本项目socket的当前WSL用户认证。

## 实际测试

- 合成5条行为输入，真实YARN应用`application_1793560382341_0001`，输出pv=2、buy=1、cart=1、fav=1；清单在config/environment.lock.json的hadoop_probe，完整日志在var/probes相应目录。
- HDFS、YARN、数据库重复初始化及停止/重启经过验证，HDFS原输出仍可读取。启用FileSystemRMStateStore后再次重启ResourceManager，FINISHED/SUCCEEDED应用状态恢复成功。
- 服务启动曾暴露缺失调度器配置、进程会话生命周期、NameNode安全模式和RM默认不持久化记录问题，已补齐配置、使用setsid、等待安全模式退出和节点RUNNING、启用私有RM状态存储。失败日志保留var，不计成功。
- CUDA探针结构含类别/行为/小时embedding、GRU及14维数值特征；参数291741，microbatch32、序列128。FP32与AMP各一次真实更新、检查点加载输出一致。allocated峰值约58.11MiB、reserved88MiB，进程RSS峰值1472.08MiB。这是小规模结构探针，不是正式batch256训练、数据效果或最终显存容量证明。
- reader读取成功且INSERT被拒绝；writer INSERT成功且CREATE TABLE被拒绝；migrator创建和清理专用环境探针表成功。检查结束无该探针表残留；业务表尚未迁移。
- `uv sync --locked --offline --link-mode hardlink`成功，无依赖漂移。新旧环境libtorch_cuda.so inode相同，确认大体积库共享。Java/Hadoop安装包官方摘要通过；源数据SHA256已采集。
- Bash语法、Python编译、XML结构与配置名唯一性通过；冻结文档完整性清单仍有效。

Codex沙箱内不暴露/dev/dxg并限制本机数据库连接；实际CUDA/数据库/Hadoop检查均通过获准的沙箱外命令执行。无需重装宿主驱动。正常WSL终端可按README运行。

## 磁盘与来源

Windows注册信息确认Ubuntu-24.04 VHDX在E:\\wsl\\Ubuntu-24.04\\ext4.vhdx。Windows侧测得空闲185423876096字节，约172.69GiB，超过120GiB预留门槛。Linux虚拟容量不能视为额外物理空间；旧的/mnt/e空间显示与本次Windows查询不同，以此次VHDX所在卷的直接查询作为宿主余量证据。

数据文件仍在/mnt/e/UserBehavior/UserBehavior.csv，没有导入或额外复制。大小3672347465字节；SHA256为46fdd7d389c1ddc7922eb7d9014af5573a4a3045da28c6c46197636873d8f1a9。尚未验证全量行数、分布、标签覆盖和来源授权材料。

## 使用与维护

从项目目录source scripts/dev-env.sh。日常启动、停止、验证命令见根README。环境需要普通WSL或具有设备/网络权限的执行路径。没有配置系统自启动，WSL重启后须重新启动。

软件依赖声明在pyproject.toml/uv.lock，工具发行锁在config/toolchain.lock.json，实际运行锁在config/environment.lock.json。后者包含版本、包列表、硬件、配置hash、源hash、探针应用与执行代码commit，不含密码。

本地Git按文档基线、Python依赖、Hadoop/Java、数据库、验证工具、启动/恢复修复及最终执行证据分别提交，提交信息中文。原始数据、安装包、模型、凭据、运行目录及虚拟环境均忽略。

后续仍需：正式业务schema与迁移、来源全量画像、J01—J06流水线、真实数据训练/评价/预测、API/UI、规模阶梯、独立备份和恢复演练。本记录只证明环境搭建范围，不能替代G-DATA、正式G-MODEL/G-GPU、业务发布或性能验收。
