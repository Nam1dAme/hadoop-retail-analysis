# M3 时间样本与分片

```bash
scripts/behaviorctl --baseline UBA-DOC-1.0.0 pipeline --batch-id <UUID> --through J06
scripts/behaviorctl --baseline UBA-DOC-1.0.0 prepare-samples --batch-id <UUID>
```

J05只在YARN按用户/时间/来源排序，维护7个锚点的有界状态；历史48h、目标24h。序列128条，数值统计取完整历史。训练4日、验证12月1日、测试12月2日、未来12月4日；未来标签文件编码-1。每个样本的user和as_of唯一。

J06只读TRAIN。保留sum/sumsq审计量，同时用Welford/Chan稳定归并总体方差，避免常量特征被浮点消减误差错误放大；常量标准差按1处理。E01以float64标准化、float32存储，每列独立.npy，每分片最多32768条。ID保留UTF8字节及int64偏移，不转浮点，不创建百万用户embedding。

导出之前以J04用户日位图独立计算样本人口和标签；之后校验完整行数、正负例、文件hash、dtype/shape、padding、有限值、ID偏移和sample_key。没有观察记录的日期不能在正式样本中静默成为负例；原始来源与授权未决在coverage中公开保留，不对外分发原始数据。

分片缓存可从HDFS重建；已存在但损坏的缓存明确失败，不静默替换。ShardDataset逐分片mmap，用seed+epoch确定顺序，不复制全量样本入内存。training_groups跨分片组织有效batch，并返回以实际样本数计算的microbatch损失权重；CPU梯度对照证明最后不足effective batch的更新也与整组均值一致。M4训练必须消费该协议。

本阶段不训练模型，experiment状态为PENDING，不伪造GPU训练曲线或测试指标。
