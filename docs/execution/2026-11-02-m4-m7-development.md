# M4—M7 开发执行记录（进行中）

引用UBA-DOC-1.0.0；执行计划与设计依据见[实施计划](2026-11-02-m4-m7-plan.md)。目标仍包含后续模型、预测发布、七页应用、规模与恢复交付，没有以界面演示代替正式训练。

## 当前已验证

- 完整27项项目回归测试通过，日志`var/production/ranking-development-regression.log`；后续时间格式与真实SQL超时专项各1项通过，最终完整28项仍待无YARN重任务窗口执行。此前25/24项日志另存。包含模型极小样本真实梯度/过拟合、权重加载、PAD因果性、外排序同分与独立公式、分片契约、真实隔离MariaDB的15个API和七页Streamlit渲染、装载中断/幂等、双连接发布CAS和回滚。
- Java既有契约在新增热点验算入口后仍通过，日志`var/production/java-contracts-m7.log`；热点入口现已通过3个真实YARN作业验证，加盐前后100000条计数一致，90000条热点分散到4桶；这是独立合成计数验证，不是生产序列加盐。
- FP32设备对照使用共同TRAIN样本hash、同初始权重与CPU生成的共同Dropout掩码；CPU/CUDA各3次200步。中位数11.5859/2.3875秒，约4.8527倍，第一步loss差0。证据在正式实验目录`benchmark.json`。这是该固定FP32实验，不等同正式AMP训练加速比。
- 正式CONSTANT、两个LOGISTIC及两个CUDA GRU候选全部完成；每个完整epoch均处理3463211条TRAIN样本，溢出跳步另记。选择在2026-11-02T12:51:58.863036Z封存，独立核验`var/production/model-verification.json`为PASS；核验全部产物与检查点输出、选择规则及指标。选中GRU学习率0.0003，第9轮为最佳验证检查点；未依据TEST重新调参。
- 正式CUDA进程在第3个完整epoch之后受控终止，原PID和启动标识核实；同模型ID从第4轮恢复且完成，无重复epoch，完整人口不变。`var/production/cuda-recovery-test.json`为PASS。另有隔离CPU训练恢复与不中断权重逐项一致测试。
- 真实桌面与390px浏览器空态截图通过，窄屏scrollWidth=390。字体源沿用旧项目缓存的IBM Plex Sans/Noto Sans SC，重新生成本项目字形子集，保留许可。正式发布后的14张七页截图现已通过，另有6张选定模型曲线/资源图及2张真实用户表单补充图。
- PyArrow26与固定NumPy1.26不兼容，首次真实表格渲染失败；已单独锁定PyArrow19.0.1，七页测试重新通过。初始环境/uv锁与恢复后实际包版本说明保存在模型实验目录，没有把UI依赖调整伪装成原始锁未变化。
- 私有HistoryServer已在19888真实就绪；修复Hadoop3.4.2的“stopped仍返回0”状态误判，启动依据状态文本及HTTP门禁。
- 实际探针发现连接池重连会把数据库端查询限时由5秒复位到0；已在每次取得连接时恢复会话限时，并以真实隔离数据库的新旧连接ID及会话变量验证。附加回归1项通过，日志`var/production/pool-reconnect-regression.log`，服务已重启载入修复。
- 控制writer、结果loader、API reader分离；真实非变更数据权限探针通过，DDL及FILE权限被拒绝。`var/production/permission-verification.json`为PASS。
- 当前PyArrow19.0.1及权限收紧后的CUDA/数据库探针重新通过，后续环境锁已更新；旧锁保留在`var/runtime-environments/pre-m7/`，正式模型归档仍保留其历史锁和依赖调整说明。
- WebHDFS真实小文件上传/下载/摘要探针通过。DataNode公布主机名而仅绑定回环，客户端已限定重定向到本机19864。完整备份/恢复仍待运行。

## 正式入口和证据

训练：`python scripts/train-models.py`。首次日志`var/production/m4-training.log`；受控恢复后日志`var/production/m4-resumed-training.log`。实验为`99a34ac7-eb4c-4b6b-a520-0c062e8289be`，读取既有完整样本缓存，不重新计算一亿行。

独立测试命令：`BEHAVIOR_INTEGRATION=1 .venv/bin/python -m pytest -q`。未显式启用时，实际数据库集成测试标为skip；不能把默认skip当作正式通过。pytest仅收集本项目tests，不收集忽略目录中的第三方下载缓存。

资源采样：`var/production/resource-telemetry.jsonl`。从启动采样时刻起记录，升级探针时有明确时间戳；不声称覆盖M1—M3早期的全部磁盘峰值。新增宿主卷余量和Swap进出页数，区分Swap已用量与实际换页活动。


## 已封存的模型结果

TEST样本888982条，其中158538条正例。下列指标来自一次封存评估，独立核验重新计算通过；效果提升有限，不能据此宣称商业收益。

| 模型 | AP | ROC-AUC | LogLoss | F1 |
|---|---:|---:|---:|---:|
| CONSTANT | 0.178337 | 0.500000 | 0.471014 | 0.302692 |
| LOGISTIC | 0.235391 | 0.575225 | 0.466999 | 0.305403 |
| GRU | 0.250702 | 0.589744 | 0.464940 | 0.308108 |

正式模型与归档清单：`var/production/models.json`；独立核验：`var/production/model-verification.json`。GRU参数291741个，两个GRU候选最大CUDA allocated为406.51MiB、reserved为522MiB；FP32设备对照与AMP正式训练分别报告。

FORECAST已对986594名历史48小时活跃用户完成评分（评分计算34.63秒，不含归档、重试与数据库装载），使用同一选中检查点、标签保持未观测，P01归档成功；run_id为`f60b0601-b404-4178-8b0e-f0f662ae1460`。首次P01归档失败原因是检查器不支持结构化NumPy描述符；已补齐平面结构化数组解析、字段类型与载荷长度校验，并通过正常、截断、冗余字节、对象类型及维度溢出测试。失败尝试保留，未改变封存模型或测试结果。


## 已发布的数据版本

版本`b6091bad-3176-4297-a48f-218f05e981e1`于2026-11-02T15:33:41.361469Z原子发布，manifest hash为`ff8b858d35bc1268d692e8209ee4e5fd151058f59a77599551fa9215e66db2fa`。分析与预测同属既有正式批次和实验。

九个数据集装载完成：event_cube864、entity_daily20711042、user_daily6968544、range_summary45、activity_daily9；TEST三模型各888982，FORECAST986594，总预测3653540。全量COUNT、两类事件守恒及未来label=NULL检查均通过；不是以装载游标替代最终扫描。

首轮装载后因数据库连接30秒读取限时导致校验失败，保持STAGING；修复为离线SQL240秒、客户端300秒的独立有界预算，从完成游标重试，无重复装载。重试耗时256.25秒，商品日表COUNT99.41秒、SUM96.80秒，明确不属于在线API时限。在线读仍限5秒。

实际表空间元数据：entity_daily数据5370773504字节、索引1709981696字节；user_daily数据2175795200、索引528367616；prediction数据1519386624、索引929988608。information_schema.TABLE_ROWS只是估算，不用于行数验收。

逐条预测独立核验、正式浏览器及压力测试尚待各自报告；发布成功不能代替这些门禁。

## 已完成的后续应用验收

正式15个GET路由、逐条预测对账、全部45个排行日期范围、Hadoop Top100、七页宽/窄屏、真实用户表单以及五分钟10并发均通过。详细证据与限制见[M5—M6验收报告](2026-11-03-m5-m6-acceptance.md)。

## 尚需完成

1. 1000万、全量规模阶梯及完整三阶梯计数器、内存、磁盘和OOM证据审计；100万条及独立合成热点已通过。
2. 完整本地压缩备份、3308私有数据库与隔离HDFS根恢复及结果比对。
3. 无YARN重任务窗口下的最终28项完整回归、最终逐条验收汇总与完整性审计。

用户已明确没有其他硬盘；C/D/E经宿主分区查询均属于Disk0。本地备份和隔离恢复即使成功，也只标本地恢复通过。独立介质备份为未验收项，不据此宣称所有交付门禁通过。
