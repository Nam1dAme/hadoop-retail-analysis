# Java MapReduce 业务计算

构建：`scripts/build-jobs.sh`；单元验算：`scripts/test-jobs.sh`。使用已锁定Java11、Hadoop3.4.2及其提供的Jackson，不额外打包Hadoop运行库。JAR按稳定文件顺序和固定时间戳生成；同源、同编译器重复构建得到相同SHA256。

执行入口为`cn.behavior.Jobs`，由`behaviorctl`编排，不手工修改运行目录。清洗事实使用SequenceFile<LongWritable,Event>，来源偏移为key；Event保留冻结契约的十个逻辑字段，并在二进制载荷内部保留原始timestamp token，以防规范化后把词法不同的输入误算成完全相同记录。拒绝记录使用压缩文本（偏移、主原因、原字节base64）。

J01按日期Reducer流式归并，避免每个原始分片为每个日期单独保留小文件；拒绝分支由Mapper输出。J02使用SortKey分组及二级排序，精确统计来源位置唯一性、三类基数、重复候选、商品多类目与类别hash碰撞，无全量ID集合。J03使用可结合的整数加法Combiner。J04按用户排序、有界9日状态计算用户日和45个连续区间，再以单独J04summary物理任务汇总。J05每用户保留7个锚点、各至多128条事件，所有48h计数仍取完整历史；J06只读TRAIN，生成14维总体均值标准差。

`cn.behavior.Files`以有界缓冲流式读取SequenceFile、逐文件计算hash/行数。它只承担产物检查与格式导出，不替代业务MapReduce。HDFS小文件、空输出、所有物理任务的真实application ID均进入清单。

本模块不训练模型；M4消费M3分片。所有源文件由本项目开发，不含原UCI交易清洗规则或订单/金额映射。
