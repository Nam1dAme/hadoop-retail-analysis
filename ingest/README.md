# 来源归档与M1质量控制

先应用migrations，再执行：

```bash
scripts/behaviorctl --baseline UBA-DOC-1.0.0 ingest --source-url https://tianchi.aliyun.com/dataset/649
scripts/behaviorctl --baseline UBA-DOC-1.0.0 profile --batch-id <UUID>
```

导入流式计算SHA256和原始逻辑行数，CRLF及无末尾换行均计数；归档后复核HDFS字节摘要。source_sha256+behavior-v1唯一注册批次。下载日期未知为NULL；source_offset为原文件字节定位，不冒充事件ID。原始数据不可变。

J01按冻结优先级拒绝，合法窗口外数据独立计数并保留；J02精确统计重复候选并保留全部事件，不假造订单或金额。raw/accepted/rejected/scope等计数必须守恒。质量不通过保留画像和Q01报告，返回退出码4；只有三重隔离合成配置可用--audit继续验算失败样例。

源文件字节边界造成的Hadoop InputSplit不是独立源文件；完整源行数和来源偏移唯一性共同校验所有记录，没有用空的二进制同步片段冒充丢行或成功数据。大文件不会在Python里加载完整行表。
