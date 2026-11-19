# 16 基线审查摘要

基线 `UBA-DOC-1.0.0`。

## 设计对齐要点

| 项目 | 设计约定 |
|---|---|
| Hadoop 全量计算 | 清洗、精确聚合与时序样本由 Java MapReduce 在 YARN 上执行 |
| PyTorch CUDA 训练 | `CONSTANT`、`LOGISTIC`、`GRU` 三模型对比，GRU 使用单卡 CUDA 混合精度训练 |
| 显存与内存控制 | 有界分片流式加载，配置 `allocated <= 6144 MiB`、`reserved <= 6656 MiB` |
| 业务语义边界 | 严格使用原始五字段，不虚构订单、金额或商品名称 |
| 时间预测闭环 | 48h 历史窗口预测未来 24h 购买，TEST 封存、FORECAST 无标签且不回流重训 |
| 原子发布与回滚 | `STAGING -> READY -> RETIRED` 状态机与 `current_release` CAS 指针切换 |
| 只读服务与页面 | FastAPI 只读接口 + 游标签名 + Streamlit 七页中文工作台 |
