# 07 购买行为预测、深度学习与CUDA

UBA-DOC-1.0.0 / FROZEN。

## 样本与防泄漏

目标及20维特征严格使用04。TRAIN锚点2017-11-27—11-30（四天），VALIDATION=12-01，TEST=12-02、12-03（12-03标签窗完整在数据内）；FORECAST=12-04。每个锚点历史48h、标签24h，左闭右开。TRAIN最后标签结束于VALIDATION锚点；VALIDATION标签结束于TEST首日；TEST末日标签在数据边界结束，不交叉目标窗。

验证/测试的历史允许包含锚点之前真实已观察的事件（含前一天标签期行为），模拟每日运行；不得把锚点之后事件加入历史。模型权重、标准化仅来自TRAIN，类别固定hash无全量词表。相同用户跨集合允许，评价声明是时间泛化而非用户完全冷启动；分别报告用户是否出现在TRAIN样本的cohort。

若来源完整性无法证明整个标签窗已覆盖，不能将缺失日期静默标为负例。TRAIN/VALIDATION/TEST总体必须各有正负两类、至少1000个样本及每类至少50条，否则正式实验门禁失败；小样例测试不受此限制。不重采样验证或测试，不负采样，不按未来行为筛选历史活跃人群。重叠历史造成样本相关，不声称IID置信区间。

## 必做模型与新增基线

| 模型 | 设计 |
|---|---|
| CONSTANT | 训练集正例率为所有样本分数；无可训练参数，是必须对照 |
| RULE | 反标准化数值特征，按 48h cart/buy/fav>0 与第 18 维>0 四个信号分 16 组；每组 TRAIN 平滑率 (正例+1)/(样本+2)，存 rules.json 并计入 checkpoint_hash；无可训练参数 |
| LOGISTIC | 20维标准化特征→线性1→logit；PyTorch训练，BCEWithLogitsLoss；衡量序列模型的增益 |
| GRU | 类别embedding(16385,16)、行为(5,8)、小时(25,4)，gap标量；29维拼接→Linear(29,64)+ReLU→单层GRU(hidden=64)；取每序列最后有效hidden，与20维数值拼接84→Linear32+ReLU+Dropout0.1→Linear1 |
| GBDT（新增基线） | 纯 NumPy 直方图梯度提升二分类：只用20维数值特征，TRAIN 固定种子抽样分位数分箱（≤64箱、uint8），深度≤4、牛顿叶子值+L2(lambda=1)、min_child_samples=200、学习率0.1、≤300棵树；VALIDATION logloss 早停 patience=20 |

padding索引0，PAD embedding固定0；使用length取最后有效输出，后部PAD不能影响结果。原始user_id/item_id不进入网络，不创建百万级用户或商品embedding；类别hash冲突是已知信息损失。GRU参数量约30万级，实施须给出精确count；参数少不等于显存峰值已验证。

模型输出logit；损失使用未加权BCEWithLogitsLoss，不先sigmoid；评价/展示sigmoid。LOGISTIC和GRU均AdamW，weight_decay=1e-4，学习率候选固定[0.001,0.0003]，默认seed=42用于选择；最多20epoch，验证AP连续3epoch无超过1e-6提升早停，保留最佳epoch。选择封存前，LOGISTIC与GRU finalist以相同超参追加seed=43、44训练，不参与选择；TEST对所有finalist及追加运行评估，models.json的seed_variance报告seed、test_ap、test_auc与test_ap的mean/std。LOGISTIC无Dropout。所有候选使用同一训练样本全集，不截断只训前若干batch。

GBDT不引入scikit-learn/lightgbm等新依赖。分位数边界只由TRAIN固定种子抽样（最多200k行）计算，分片按mmap顺序读取、全量TRAIN分箱矩阵常驻内存（约70MB），直方图用`np.bincount`，不使用Python逐行循环。每棵树在上一轮logit上计算梯度/海森，节点增益按`G²/(H+lambda)`计算，叶子值为牛顿步，最终分数为`base_score + 学习率×各树叶值`的sigmoid。VALIDATION只用于逐轮logloss早停与阈值选择，TEST/FORECAST只做加载后打分；早停保留验证logloss最好的树数。相同样本与种子生成的JSON逐位一致，其sha256即checkpoint_hash；`model.json`记录箱边界、树节点与base_score，`history.json`记录逐树日志。

GRU正式训练CUDA、FP16 autocast+GradScaler，权重/优化器主状态FP32；损失在FP32计算，裁剪梯度norm≤1在unscale之后进行。LOGISTIC正式训练可CPU，记录实际设备。TF32关闭；Python/NumPy/torch种子和确定性开关必须记录；不承诺跨CUDA版本逐位相等。无CUDA不可降级冒充GPU完成。

## 8GB单卡与数据加载

CUDA GRU 从 micro_batch=4096 开始预检，到 32 为止依次减半。CPU LOGISTIC 使用 512，显式 CUDA LOGISTIC 从 8192 开始预检。GRU 预检使用长度全为 128 的输入，记录实际 dtype、成功更新及显存峰值。effective_batch=max(512, micro_batch)，两者都计入训练配置身份。学习率仍为 0.001 或 0.0003。更大的有效批量会减少每轮 AdamW 更新次数，收敛和验证 AP 以实测为准。每次更新按实际样本数归一化，包括末尾不足整批的样本。overflow 跳步计入报告，非有限损失直接失败。

每个 epoch 由 seed+epoch 决定分片和片内顺序。GRU 在分片内组成长度相近的批次，再打乱批次顺序，序列只读取到批次的最大有效长度。CUDA 训练和神经网络打分使用单线程预取下一组，复用两套 pinned 内存，通过拷贝流传输特征，序列长度留在 CPU。mmap 按分片读取，每个 epoch 处理完整 TRAIN 人口。打分按原始样本顺序写出 FP32 模型的分数，并核对样本人口和顺序哈希。

完成每epoch保存CPU可加载state_dict、optimizer/scaler/RNG状态、epoch和输入hash，写临时文件后原子更名。中断恢复从最后完整epoch，未完成epoch重跑，不假装位级接续；恢复不得改变样本或参数。检查点仅来自本项目可信产物，不加载未知pickle。

## 评价、选择和预测

主指标Average Precision（AP，非梯形PR-AUC）；按不同分数组降序，AP=sum((Recall_k-Recall_(k-1))*Precision_k)，同分一起处理。另报ROC-AUC（同分平均秩）、LogLoss（概率截到[1e-7,1-1e-7]）、Brier、Precision/Recall/F1、混淆矩阵、固定10个等宽概率桶的校准图；top-k按分数降序、同分按sample_key取前ceil(k×n)，k∈{1%,5%,10%}报precision/recall/lift（lift=precision/实际正例率）。按全样本统一计算，不平均batch指标；分数字段带as_of，除ALL/SEEN_TRAIN/UNSEEN_TRAIN外按Asia/Shanghai日期报DAY:*；磁盘外排分数可用于精确AP/AUC，禁止把全量预测装入Python对象列表。

阈值仅在VALIDATION网格0.005—0.995步长0.005上选F1最大，平局选较大阈值；score≥threshold为正。候选配置按VALIDATION ALL AP最高选，差≤1e-6按LogLoss低、参数少、预定义顺序CONSTANT/LOGISTIC/GRU/RULE/GBDT及学习率列表顺序决胜。最终selected可为简单基线，但GRU训练与对照仍必须完成。

RULE基线先以TRAIN的mean/std反标准化，四个信号为48h cart(第10维)>0、48h buy(第9维)>0、48h fav(第11维)>0、第18维>0；VALIDATION选阈值并与其他finalist一起参与选择，TEST封存后一次评估。GBDT finalist同样以VALIDATION选阈值，与其余四个算法共同进入select()，封存后与其他finalist一起在TEST上只评估一次。

先封存全部配置、阈值和selected，再对TEST一次评估；同配置失败重试保留记录，不得据TEST调参。cohort单类别时AP/AUC=NULL并给原因，不伪报0；其余可定义指标照报。总体测试必须两类。无预测正例时Precision=0；无真实正例时Recall=NULL；Precision+Recall=0且Recall可定义时F1=0，Recall不可定义时F1=NULL。校准桶左闭右开，最后一桶含1，空桶计数0、均值NULL。

FORECAST使用selected的同一评估checkpoint（只在TRAIN学习），以12-04 00:00为锚点评分过去48h活跃人群，预测12-04全天；原数据到12-04边界为止，label=NULL。不进行FULLFIT，不把TEST参与再训练；页面明确这是历史数据上的未来窗口演示，不是对当前现实日期的预测。TEST页可查全部finalist（含GBDT，若被选中则FORECAST也使用GBDT），FORECAST仅默认模型。

## CPU/GPU可比实验

固定GRU FP32，micro_batch=64、无累积，CPU线程4，关闭TF32；选TRAIN样本hash排序最小的65536条（不足则全量，但须≥4096），固定顺序。CPU/CUDA各3次独立初始化，seed42；20步预热后重置权重、优化器和随机状态，测200训练步，数据不足循环取样，记录实际处理条数。计时包含host-to-device、forward/backward/optimizer；不含文件预加载、预热，另报端到端时间。CUDA计时两端synchronize；正式AMP训练性能单独报告，不混入FP32加速比。

记录三次中位耗时、样本/秒、allocated/reserved峰值、整卡占用、进程RSS、型号、驱动、torch CUDA runtime、功耗/温度可获得值；无法取得的遥测为null并解释。speedup=CPU中位耗时/GPU中位耗时，不要求>1。相同初始权重与batch第一步loss差≤1e-4；训练全过程有限、有真实梯度及参数更新；不强制每步loss下降。

产物：manifest、config、hardware、environment锁引用、normalization、training_history、state_dict、resume_checkpoint、validation_metrics、封存test_metrics、prediction清单。CONSTANT/RULE保存rules.json（正例率/16组规则表），不伪造state_dict；LOGISTIC/GRU保存state_dict，resume_checkpoint仅用于可训练模型；GBDT保存model.json（箱边界/树节点/base_score，校验sha256）与history.json，不伪造state_dict或resume_checkpoint。文中沿用checkpoint对固定基线指沿用同一rules.json，对GBDT指同一model.json。算法、样本hash、split、阈值、selected原因、实际精度、种子方差和资源降级均可审计。FORECAST页面注明分数为模型打分、未做概率校准，且输入为周末历史、训练集仅含工作日。

技术参考：[PyTorch AMP](https://docs.pytorch.org/docs/2.15/notes/amp_examples.html)说明autocast与梯度缩放；[DataLoader](https://docs.pytorch.org/docs/2.15/data.html)说明分批加载与worker行为。具体结构、预算和选择规则均为本项目设计，不是官方性能保证。
