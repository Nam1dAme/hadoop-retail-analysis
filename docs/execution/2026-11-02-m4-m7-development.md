# M4—M7 开发执行记录（进行中）

引用UBA-DOC-1.0.0；执行计划与设计依据见[实施计划](2026-11-02-m4-m7-plan.md)。目标仍包含后续模型、预测发布、七页应用、规模与恢复交付，没有以界面演示代替正式训练。

## 当前已验证

- 24项项目回归测试通过，日志`var/production/development-regression.log`。包含模型极小样本真实梯度/过拟合、权重加载、PAD因果性、外排序同分与独立公式、分片契约、真实隔离MariaDB的15个API和七页Streamlit渲染、装载中断/幂等、双连接发布CAS和回滚。
- Java既有契约在新增热点验算入口后仍通过，日志`var/production/java-contracts-m7.log`；新增热点入口尚未提交真实YARN，不能标为通过。
- FP32设备对照使用共同TRAIN样本hash、同初始权重与CPU生成的共同Dropout掩码；CPU/CUDA各3次200步。中位数11.5859/2.3875秒，约4.8527倍，第一步loss差0。证据在正式实验目录`benchmark.json`。这是该固定FP32实验，不等同正式AMP训练加速比。
- 正式CONSTANT、两个LOGISTIC及两个CUDA GRU候选全部完成；每个完整epoch均处理3463211条TRAIN样本，溢出跳步另记。选择在2026-11-02T12:51:58.863036Z封存，独立核验`var/production/model-verification.json`为PASS；核验全部产物与检查点输出、选择规则及指标。选中GRU学习率0.0003，第9轮为最佳验证检查点；未依据TEST重新调参。
- 正式CUDA进程在第3个完整epoch之后受控终止，原PID和启动标识核实；同模型ID从第4轮恢复且完成，无重复epoch，完整人口不变。`var/production/cuda-recovery-test.json`为PASS。另有隔离CPU训练恢复与不中断权重逐项一致测试。
- 真实桌面与390px浏览器空态截图通过，窄屏scrollWidth=390。字体源沿用旧项目缓存的IBM Plex Sans/Noto Sans SC，重新生成本项目字形子集，保留许可。正式发布后的14张七页截图尚待执行。
- PyArrow26与固定NumPy1.26不兼容，首次真实表格渲染失败；已单独锁定PyArrow19.0.1，七页测试重新通过。初始环境/uv锁与恢复后实际包版本说明保存在模型实验目录，没有把UI依赖调整伪装成原始锁未变化。
- 私有HistoryServer已在19888真实就绪；修复Hadoop3.4.2的“stopped仍返回0”状态误判，启动依据状态文本及HTTP门禁。
- 控制writer、结果loader、API reader分离；真实非变更数据权限探针通过，DDL及FILE权限被拒绝。`var/production/permission-verification.json`为PASS。
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

正式模型与归档清单：`var/production/models.json`；独立核验：`var/production/model-verification.json`。GRU参数291741个，峰值CUDA allocated约408MiB、reserved约508MiB；FP32设备对照与AMP正式训练分别报告。

FORECAST已对986594名历史48小时活跃用户完成评分，使用同一选中检查点、标签保持未观测，P01归档成功；run_id为`f60b0601-b404-4178-8b0e-f0f662ae1460`。首次P01归档失败原因是检查器不支持结构化NumPy描述符；已补齐平面结构化数组解析、字段类型与载荷长度校验，并通过正常、截断、冗余字节、对象类型及维度溢出测试。失败尝试保留，未改变封存模型或测试结果。

## 尚需完成

1. 同批次分析/预测结果装载、校准报告装载、完整校验及正式READY发布。
3. 真实发布版本的全部API、七页桌面/窄屏、持续五分钟十并发及冷查询测量；发现性能问题后依据实测优化。
4. 确定性100万、1000万、全量真实YARN分析阶梯、热点加盐计数等价、Shuffle与倾斜及空间峰值报告。
5. 完整本地压缩备份、3308私有数据库与隔离HDFS根恢复及结果比对、运行手册与逐项验收汇总。

用户已明确没有其他硬盘；C/D/E经宿主分区查询均属于Disk0。本地备份和隔离恢复即使成功，也只标本地恢复通过。独立介质备份为未验收项，不据此宣称所有交付门禁通过。
