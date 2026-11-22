# 15 来源与参考边界

基线 `UBA-DOC-1.0.0`。

## 数据与外部参考

- **天池 UserBehavior 数据集**：<https://tianchi.aliyun.com/dataset/649>。原始 CSV 无表头，共五列（`user_id, item_id, category_id, behavior_type, timestamp`），本地实测 `3,672,347,465` 字节（约 3.42 GiB），`100,150,807` 行。分析窗口为北京时间 `2017-11-25` 至 `2017-12-04`（不含）。
- **Hadoop 3.4.2**：<https://hadoop.apache.org/docs/r3.4.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html>
- **PyTorch AMP 与数据加载**：<https://docs.pytorch.org/docs/stable/notes/amp_examples.html>、<https://docs.pytorch.org/docs/stable/data.html>
- **NVIDIA CUDA on WSL**：<https://docs.nvidia.com/cuda/wsl-user-guide/index.html>

## 事实与约束边界

- `buy` 仅代表购买行为记录，原始数据不包含订单号、商品价格或商品名称。
- 性能实测结果仅对应记录时的硬件、数据集与并发配置；模型选择与阈值仅使用验证集，测试集仅用于封存评价。
- 所有验证报告均应记录执行命令、时间、commit 与环境锁 hash，不记录数据库口令或凭据文件内容。
