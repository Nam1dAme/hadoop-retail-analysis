# 15 来源、硬件与证据边界

UBA-DOC-1.0.0 / FROZEN；调查日期2026-11-02。

## 本地与用户证据

| 编号 | 证据 | 可支持的结论 | 不能支持的结论 |
|---|---|---|---|
| E01 | 本轮读取/home/hadoop/hadoop-lab/docs/README、00、07、11、BASELINE-MANIFEST；本地git HEAD=3ef5040ae4ad1fd0527b9b9044b38c0cb7cdf770 | 原文档00—16形式、旧交易推荐设计可供参考 | 新系统已实现或旧性能适用于新数据 |
| E02 | 前轮git ls-remote与本地相同HEAD；本轮未再次联网git核对 | 前次调查时远程与本地提交一致 | 当前远程永不变化 |
| E03 | 本轮stat源文件3672347465字节，mtime=2018-05-10；head显示无表头五列 | 约3.42GiB，开头符合user/item/category/type/ts结构 | 精确行数、全量合法率、下载时间、完整来源hash |
| E04 | 本轮free约7.4GiB总内存、6.4GiB可用；df Linux约879GiB空闲、E约89GiB | 当前工具环境瞬时资源可见值 | Windows宿主总RAM、物理磁盘独立余量 |
| E05 | 前轮lscpu Ryzen9 7940HX、16核32线程；YARN配置3072MiB/4vcores本轮复读 | CPU可见拓扑和仓库配置 | 32个训练核可同时用满、当前运行YARN等于该文件 |
| E06 | 用户明确GPU为RTX4060 Laptop、8GB显存 | 设计目标按本机单卡制定 | 已测实际CUDA可用或峰值 |
| E07 | 前轮nvidia-smi提示GPU访问被操作系统阻止 | 当时工具会话未取得GPU证据 | 用户无GPU或普通WSL一定不能训练 |

前轮曾发起全量CSV扫描，未取得完成输出；本轮不依赖该任务、不将其记作成功。全量行数、用户/商品/类目基数、四行为分布、时间极值、来源SHA、重复及异常率均待M0正式画像。不能直接引用网上标称规模替代本文件实测。

## 官方与原项目来源

- [原项目仓库](https://github.com/Nam1dAme/hadoop-retail-recommender)：本地代码和文档是本次具体设计比较依据；参考不等于继承其UCI语义。
- [天池UserBehavior数据集页面](https://tianchi.aliyun.com/dataset/649)：本轮网页工具只返回标题，完整字段说明/许可正文未获取。字段释义是待来源材料复核的输入契约；2017-11-25—12-03为设计选定分析期，不写成已经全量验证的实际时间范围。
- [Hadoop3.4.2 MapReduce官方文档](https://hadoop.apache.org/docs/r3.4.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)：支持InputSplit、Mapper/Reducer、Shuffle、调度及失败重试的架构依据；不提供本机吞吐保证。
- [PyTorch AMP官方示例](https://docs.pytorch.org/docs/2.15/notes/amp_examples.html)：autocast与GradScaler机制参考；本项目具体batch和预算是设计值。
- [PyTorch数据加载官方文档](https://docs.pytorch.org/docs/2.15/data.html)：分批加载和多worker行为参考；本基线默认worker0，有界缓存和mmap由项目实现。
- [NVIDIA CUDA on WSL指南](https://docs.nvidia.com/cuda/wsl-user-guide/index.html)：WSL使用宿主驱动的部署参考，版本兼容以M0实装验证。

访问日期2026-11-02；stable PyTorch页面本轮重定向到2.15文档，但本项目不据此声称已安装该版本。没有使用二手博客统计作为实测证据。

## 事实、设计、假设分离

事实：源字节、可见RAM、用户指定GPU、原代码中的集中式路径。设计：时间窗、标签、GRU结构、资源上限、阶段顺序及性能门槛。推断：原集中式字典可能超内存、Windows挂载读盘可能影响吞吐、类别hash可能损失信息。只有后续实测才能将推断标为已验证。

每个未来证据文件需记录命令、时间、主机/运行环境、原始输出摘要、baseline和hash。禁止记录凭据；不伪造GPU日志、模型曲线、下载记录或验收PASS。
