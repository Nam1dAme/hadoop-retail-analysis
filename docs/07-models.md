# 07 购买行为预测、深度学习与CUDA

UBA-DOC-1.0.0 / FROZEN。

## 样本与防泄漏

目标及14维特征严格使用04。TRAIN锚点2017-11-27—11-30（四天），VALIDATION=12-01，TEST=12-02；FORECAST=12-04。每个锚点历史48h、标签24h，左闭右开。TRAIN最后标签结束于VALIDATION锚点；VALIDATION标签结束于TEST锚点，不交叉目标窗。

验证/测试的历史允许包含锚点之前真实已观察的事件（含前一天标签期行为），模拟每日运行；不得把锚点之后事件加入历史。模型权重、标准化仅来自TRAIN，类别固定hash无全量词表。相同用户跨集合允许，评价声明是时间泛化而非用户完全冷启动；分别报告用户是否出现在TRAIN样本的cohort。

若来源完整性无法证明整个标签窗已覆盖，不能将缺失日期静默标为负例。TRAIN/VALIDATION/TEST总体必须各有正负两类、至少1000个样本及每类至少50条，否则正式实验门禁失败；小样例测试不受此限制。不重采样验证或测试，不负采样，不按未来行为筛选历史活跃人群。重叠历史造成样本相关，不声称IID置信区间。

## 三种必做模型

| 模型 | 设计 |
|---|---|
| CONSTANT | 训练集正例率为所有样本分数；无可训练参数，是必须对照 |
| LOGISTIC | 14维标准化特征→线性1→logit；PyTorch训练，BCEWithLogitsLoss；衡量序列模型的增益 |
| GRU | 类别embedding(16385,16)、行为(5,8)、小时(25,4)，gap标量；29维拼接→Linear(29,64)+ReLU→单层GRU(hidden=64)；取每序列最后有效hidden，与14维数值拼接78→Linear32+ReLU+Dropout0.1→Linear1 |

padding索引0，PAD embedding固定0；使用length取最后有效输出，后部PAD不能影响结果。原始user_id/item_id不进入网络，不创建百万级用户或商品embedding；类别hash冲突是已知信息损失。GRU参数量约30万级，实施须给出精确count；参数少不等于显存峰值已验证。

模型输出logit；损失使用未加权BCEWithLogitsLoss，不先sigmoid；评价/展示sigmoid。LOGISTIC和GRU均AdamW，weight_decay=1e-4，学习率候选固定[0.001,0.0003]、seed=42；最多10epoch，验证AP连续2epoch无超过1e-6提升早停，保留最佳epoch。LOGISTIC无Dropout。所有候选使用同一训练样本全集，不截断只训前若干batch。

GRU正式训练CUDA、FP16 autocast+GradScaler，权重/优化器主状态FP32；损失在FP32计算，裁剪梯度norm≤1在unscale之后进行。LOGISTIC正式训练可CPU，记录实际设备。TF32关闭；Python/NumPy/torch种子和确定性开关必须记录；不承诺跨CUDA版本逐位相等。无CUDA不可降级冒充GPU完成。

## 8GB单卡与数据加载

默认micro_batch=256，gradient_accumulation=2，effective_batch=512；若预检超09预算，按128×4、64×8、32×16顺序缩小，effective_batch不变并记录配置。缩至32仍失败则门禁失败，不静默减序列长度/数据量。每次更新loss按实际样本数归一化，最后不足512仍更新且正确归一化；overflow跳步计入报告，非有限损失直接失败。

每epoch按seed+epoch确定分片顺序、片内索引顺序；mmap读取，不加载全表；num_workers=0默认，pin_memory=true，有界预取≤2个micro_batch。调至2 workers必须保证每样本恰好一次、RSS和吞吐测量合格，不能因worker复制数据造成重复。样本总处理数必须等于每epoch TRAIN样本数。

完成每epoch保存CPU可加载state_dict、optimizer/scaler/RNG状态、epoch和输入hash，写临时文件后原子更名。中断恢复从最后完整epoch，未完成epoch重跑，不假装位级接续；恢复不得改变样本或参数。检查点仅来自本项目可信产物，不加载未知pickle。

## 评价、选择和预测

主指标Average Precision（AP，非梯形PR-AUC）；按不同分数组降序，AP=sum((Recall_k-Recall_(k-1))*Precision_k)，同分一起处理。另报ROC-AUC（同分平均秩）、LogLoss（概率截到[1e-7,1-1e-7]）、Brier、Precision/Recall/F1、混淆矩阵、固定10个等宽概率桶的校准图。按全样本统一计算，不平均batch指标；磁盘外排分数可用于精确AP/AUC，禁止把全量预测装入Python对象列表。

阈值仅在VALIDATION网格0.01—0.99步长0.01上选F1最大，平局选较大阈值；score≥threshold为正。候选配置按VALIDATION ALL AP最高选，差≤1e-6按LogLoss低、参数少、预定义顺序CONSTANT/LOGISTIC/GRU及学习率列表顺序决胜。最终selected可为简单基线，但GRU训练与对照仍必须完成。

先封存全部配置、阈值和selected，再对TEST一次评估；同配置失败重试保留记录，不得据TEST调参。cohort单类别时AP/AUC=NULL并给原因，不伪报0；其余可定义指标照报。总体测试必须两类。无预测正例时Precision=0；无真实正例时Recall=NULL；Precision+Recall=0且Recall可定义时F1=0，Recall不可定义时F1=NULL。校准桶左闭右开，最后一桶含1，空桶计数0、均值NULL。

FORECAST使用selected的同一评估checkpoint（只在TRAIN学习），以12-04 00:00为锚点评分过去48h活跃人群，预测12-04全天；原数据到12-04边界为止，label=NULL。不进行FULLFIT，不把TEST参与再训练；页面明确这是历史数据上的未来窗口演示，不是对当前现实日期的预测。TEST页可查三模型，FORECAST仅默认模型。

## CPU/GPU可比实验

固定GRU FP32，micro_batch=64、无累积，CPU线程4，关闭TF32；选TRAIN样本hash排序最小的65536条（不足则全量，但须≥4096），固定顺序。CPU/CUDA各3次独立初始化，seed42；20步预热后重置权重、优化器和随机状态，测200训练步，数据不足循环取样，记录实际处理条数。计时包含host-to-device、forward/backward/optimizer；不含文件预加载、预热，另报端到端时间。CUDA计时两端synchronize；正式AMP训练性能单独报告，不混入FP32加速比。

记录三次中位耗时、样本/秒、allocated/reserved峰值、整卡占用、进程RSS、型号、驱动、torch CUDA runtime、功耗/温度可获得值；无法取得的遥测为null并解释。speedup=CPU中位耗时/GPU中位耗时，不要求>1。相同初始权重与batch第一步loss差≤1e-4；训练全过程有限、有真实梯度及参数更新；不强制每步loss下降。

产物：manifest、config、hardware、environment锁引用、normalization、training_history、state_dict、resume_checkpoint、validation_metrics、封存test_metrics、prediction清单。CONSTANT保存rules.json（训练正例数/样本数/率），不伪造state_dict；LOGISTIC/GRU保存state_dict，resume_checkpoint仅用于可训练模型。文中沿用checkpoint对CONSTANT指沿用同一rules.json。算法、样本hash、split、阈值、selected原因、实际精度和资源降级均可审计。

技术参考：[PyTorch AMP](https://docs.pytorch.org/docs/2.15/notes/amp_examples.html)说明autocast与梯度缩放；[DataLoader](https://docs.pytorch.org/docs/2.15/data.html)说明分批加载与worker行为。具体结构、预算和选择规则均为本项目设计，不是官方性能保证。
