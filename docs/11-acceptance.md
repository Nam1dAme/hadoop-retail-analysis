# 11 测试、验收与证据门禁

UBA-DOC-1.0.0 / FROZEN。以下测试全部是未来要求，本轮只通过文档审查，不声称业务测试已运行。

## 门禁

G-DOC→G-ENV→G-DATA→G-MR→G-MODEL→G-GPU→G-RELEASE→G-APP→G-RECOVERY。可并行推进不依赖GPU的开发，但G-GPU不可删除或用CPU代替。质量、性能、效果分别报告；GRU未胜出不意味着工程失败，未真实训练则失败。

## 手算样例A：行为与重复

日期均北京时间，用户/商品/类目ID分别用1、2、3等数字串。行：11-25 10:00用户1商品10类目100 pv；同日10:01同用户商品buy；11:00用户2商品20类目200 cart；11:01用户2商品20 pv；上述最后一行完全重复一次；11-26 09:00用户1商品10 fav；11-26 10:00用户3商品30 pv（类目300）；另加一行behavior=oops、一行合法2018-01-01 pv。

期望raw=9、accepted=8、rejected=1、in_scope=7、outside=1；窗口内pv=4,buy=1,cart=1,fav=1；duplicate_extra=1；区间UV=3。11-25 UV=2、pv_users=2、pv_buy_users=1、比例0.5；11-26 UV=2；11-25次日复访用户1人、复访率0.5。完整观察期末日次日复访为null。此样例故意超质量阈值，不允许正式发布；隔离测试不以其替代真实数据。

## 手算样例B：时间与标签

锚点t=2017-12-02 00:00；用户1在t-48h有pv，在t-1秒有cart，在t有buy；历史包括前两条，不包括buy，y=1。用户2仅在t+1小时有buy：历史为空，不生成样本。用户3历史有pv、buy恰在t+24h：y=0。用户4历史有buy、目标日无buy：y=0。t-48h-1秒不入历史；未来标签窗事件不得增加特征计数。超过128条历史只截序列，不截14维统计。

TRAIN最后锚点11-30的标签终点为12-01；VALIDATION标签终点12-02；TEST终点12-03；FORECAST12-04标签编码-1，API/DB为null。改变t之后的非标签特征数据不应改变该样本历史，改变TEST标签窗数据不能改变normalization或模型权重。

## 手算样例C：模型指标

四条按分数降序：score=[0.9,0.8,0.2,0.1]，label=[1,0,1,0]。AP=(1+2/3)/2=5/6；ROC-AUC=3/4；Brier=(0.01+0.64+0.64+0.01)/4=0.325；阈值0.5时TP=1,FP=1,FN=1,TN=1，P/R/F1均0.5。全同分且两正两负：AP=0.5、AUC=0.5。单类别cohort的AP/AUC按07返回null和原因。

## 测试清单

| ID | 必须证明的条件 |
|---|---|
| T01 | CSV分片跨行、CRLF、无末尾换行、空行、非法UTF8/ID/type/时间解析；原字节hash与归档一致 |
| T02 | 样例A整数零误差；拒绝主原因唯一；重复候选保留；来源位置唯一 |
| T03 | 同源同policy幂等；改源新batch；时间窗口内外、商品多类目报告；全量画像与守恒 |
| T04 | 日/区间UV不相加、pv∩buy区间逻辑、复访末日null、排行平局正确 |
| T05 | 真实YARN J01—J06所有物理子任务ID与终态、_SUCCESS、输入输出manifest齐全；不能只有WordCount |
| T06 | 用户超长历史内存有界；多Reducer与1Reducer语义一致；热点加盐前后计数一致 |
| T07 | 样例B、标签边界、PAD、序列截断、14维统计、标准化TRAIN-only和cohort识别 |
| T08 | 每epoch样本恰好一次；分片mmap、worker无重复；最后不满有效batch的损失归一化正确 |
| T09 | CONSTANT精确正例率，LOGISTIC/GRU梯度更新、有限loss、checkpoint加载输出一致；独立极小平衡样本能过拟合证明训练链路有效 |
| T10 | 样例C及同分、无正例、概率裁剪；全局指标不是batch平均；外排与小样本内存实现一致 |
| T11 | 仅VALIDATION选参/阈值/模型，TEST封存；TEST失败重试配置不变；无测试后调参 |
| T12 | 实际RTX 4060 Laptop单卡CUDA tensor/forward/backward、AMP/scaler、实际模型训练全集及峰值满足NF-03，无CPU冒充 |
| T13 | FP32 CPU/CUDA各3次对照，共同样本/初始权重/顺序/200步/同步计时；可报告GPU更慢 |
| T14 | FORECAST人口等于历史48h活跃用户、无遗漏/重复、label=NULL、沿用selected checkpoint，TEST三模型可查 |
| T15 | 中断epoch恢复、YARN失败、rename后DB失败、失锁、重试及发布并发不重复不混版 |
| T16 | API全部端点类型/NULL/范围/错误/游标/只读约束；七页空态及预测窗口解释 |
| T17 | NF-04三时限、NF-05十并发与p95、NF-08规模阶梯、整体内存磁盘阈值与无OOM |
| T18 | 原子发布/回滚、保留引用保护、独立备份与隔离恢复RTO≤4h及结果hash一致 |
| T19 | 用户ID不当数值、无凭据泄漏、路径/SQL注入、reader不可写、未知模型文件拒绝 |
| T20 | 需求追踪、全文件哈希/链接、来源证据与真实/合成/未验证标记完整 |

数值公式绝对误差≤1e-6；同设备checkpoint加载预测最大绝对差≤1e-6（FP32评估）；CPU/GPU第一步loss容差按07。模型正式loss不要求单调下降；不得用随机分数通过指标接口。

规模样本：100万/1000万条确定性记录hash抽样用于吞吐及计数测试，分片选择不取文件前N行。模型调试另按用户hash抽样保留完整时序，不能用记录抽样的断裂历史作为真实效果证据。全量为完整文件，合成热点数据单独目录、单独报告。

每条证据包含test_id、baseline、代码commit、环境hash、输入hash、执行命令/操作、run/experiment/release、UTC时间、actual、PASS/FAIL/NOT_RUN和证据路径。mock仅用于单元测试；真实YARN/CUDA/DB恢复不能由mock替代。NOT_RUN不计通过。
