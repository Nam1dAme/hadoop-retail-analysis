# 09 环境、部署与单机容量

基线 `UBA-DOC-1.0.0`。

## 目标环境与依赖锁

- 核心工具链：Hadoop 3.4.2、Java 11、Python 3.12（`uv.lock`）、PyTorch 2.x（CUDA 12.x）、MariaDB 10.11、FastAPI、Streamlit。
- 实际依赖版本与硬件信息通过 `scripts/check-environment.py` 与 `scripts/freeze-environment.py` 写入 `config/environment.lock.json`，未通过环境门禁前不执行正式发布。
- WSL2 下通过宿主 NVIDIA 驱动映射访问 CUDA（不要在 WSL 内安装覆盖宿主映射的 Linux 显示驱动），详见 [NVIDIA WSL 指南](https://docs.nvidia.com/cuda/wsl-user-guide/index.html)。

## 本机资源配置起点

| 项目 | 初始预算/约束 |
|---|---|
| Hadoop | 单NameNode/DataNode/ResourceManager/NodeManager，副本1，block128MiB |
| YARN | memory3072MiB，vcores4，AM512MiB；Map768MiB/heap512MiB，Reduce1024MiB/heap768MiB；JVM堆不能占满容器 |
| 守护进程 | NN256MiB、DN256MiB、RM256MiB、NM256MiB堆起点，进程RSS另测，不等同堆预算 |
| MariaDB | buffer_pool256MiB起点、连接上限30；禁止训练同时做大批装载 |
| GPU训练 | allocated≤6144MiB，reserved≤6656MiB；还需实测设备总空闲及桌面占用；每步失败不能仅靠empty_cache解释 |
| CPU/RAM | 训练CPU线程4，RSS≤3GiB，DataLoader workers0；整个环境MemAvailable保留≥1GiB |
| 并发 | 全局单个重任务，默认关闭UI后跑全量重任务；API可保留服务 |

训练时无活动YARN应用，可停止本项目RM/NM以释放内存，但不得停止旧项目服务；HDFS保留用于归档，缓存训练不逐batch读HDFS。上述预算是调度起点，容器和守护进程堆总和不代表实际RSS。需要在M0测守护进程常驻开销；不足时在不改业务语义下减少并发/容器数，仍不足则报告环境门禁失败，由用户决定扩内存。

## 磁盘预算

实测源3,672,347,465字节=约3.42GiB。源文件规模不等于解压内存；100M行×5×8字节约3.73GiB，仅裸数字，不能推论Python对象可装进内存。

先按raw副本4GiB、facts及质量15GiB、聚合/用户日10GiB、样本与本地缓存40GiB、Shuffle20GiB、模型/DB/日志/保留版本15GiB预留，总计104GiB；这是保守规划，不是实测文件大小。样本字段约1.4KiB/条（字符串与文件开销另计），样本行数上界为7×不同用户数，实际由画像得出；HDFS归档与缓存可能双份，应计入预算。

Linux df显示879GiB、E盘89GiB不能相加；确认VHDX所在宿主卷实际空闲。正式全量前要求宿主与Linux两侧可用均≥120GiB，或以1000万行实测放大估计峰值×1.5并加20GiB保底重新给出资源记录，不能仅修改阈值而无测量。运行可用低于20GiB停止新阶段，低于10GiB受控终止写密集任务。旧数据自动清理前必须满足10保留策略。

## 部署顺序与门禁

1. 核实CPU/RAM/宿主盘/源stat/hash、GPU型号和实际total_memory、驱动与WSL；用户提供的8GB不转换成虚构实测MiB。
2. 在新项目独立路径建立依赖锁，校验Java业务JAR与Hadoop；不全局覆盖旧环境。
3. 独立HDFS命名空间、数据库、账号、端口和日志目录；若复用已有HDFS守护进程，必须确认兼容并使用独立根，不执行namenode format。
4. 执行真实YARN小业务作业、MariaDB迁移/只读权限测试。
5. CUDA tensor 分配、GRU forward/backward/update、checkpoint 往返、AMP 与 FP32 预检、峰值遥测。
6. 100 万 → 1000 万 → 全量数据阶梯，锁定实测配置。

所有路径、端口与连接参数均通过 `config/development.toml` 与 `scripts/dev-env.sh` 注入，避免在代码中硬编码本机路径或端口。
