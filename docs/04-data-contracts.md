# 04 数据与文件契约

UBA-DOC-1.0.0 / FROZEN。

## 原始字段与解析

CSV为UTF-8、无表头、五列无引号简单逗号分隔；CRLF/LF均接收；最后一行无换行仍计一条；空行计拒绝行。若实际发现引号、编码或格式差异，隔离并报告，不猜测解析。源字节永不改写。

| 字段 | 约束 | 语义 |
|---|---|---|
| user_id | 非空十进制数字，1—20字符，保留原字符串 | 数据集用户标识，不是真实身份 |
| item_id | 同上 | 商品标识，无名称价格 |
| category_id | 同上 | 类目标识，无名称层级 |
| behavior_type | 严格pv/buy/cart/fav | 浏览/购买/加购/收藏行为记录 |
| event_ts | 可解析有符号int64的Unix秒 | 转换UTC，再转Asia/Shanghai |

不去空格、不去前导零、不大小写纠正；不合规则拒绝。时间超出可转换0001—9999年范围拒绝；可转换但超出分析期的记录属于ACCEPTED_OUTSIDE，不混同损坏记录。

规范事实字段：source_sha256、source_offset（源文件行首字节偏移int64）、user_id、item_id、category_id、behavior_type、event_ts、event_date、event_hour、in_scope。source_sha256+source_offset是来源主键，不是业务事件ID。未压缩CSV由Hadoop RecordReader提供字节偏移；边界跨split只处理一次，必须测试。

观察区间固定 **[2017-11-25T00:00:00+08:00, 2017-12-04T00:00:00+08:00)**。原始保留全部，事实层保留合法窗口内外记录；分析与模型只取in_scope=true。来源完整性不能仅靠最大时间戳判断；必须有来源说明、每日覆盖与异常报告。

拒绝原因按优先级唯一赋值：BAD_ENCODING、BAD_FIELD_COUNT、BAD_ID、BAD_BEHAVIOR、BAD_TIMESTAMP。多问题可列warnings，primary_reason仅一个。重复候选是合法记录五字段完全相同的组，extra_count=sum(max(group_size-1,0))；保留所有行，不能把同秒记录自动删除。商品多类目按事件实际类别保留并计冲突商品数，不制造静态唯一类目映射。

质量等式：raw_rows=accepted_rows+rejected_rows；accepted_rows=in_scope_rows+outside_rows；in_scope_rows=sum(四行为计数)。空源、任何计数不守恒、任何无记录的源分片、主键重复阻止发布。默认rejected/raw>0.1%或outside/accepted>1%阻止正式发布；分母0直接失败。超过阈值只允许审计，不悄悄放宽。重复率仅报告；训练/验证/测试单类别另按07失败。

## 分析口径

- 行为量：记录数，重复候选照计。buy_count不是订单数。
- UV：范围内不同user_id精确去重；不得相加每日UV当区间UV。
- 日浏览用户购买比例：当日既有pv又有buy的用户数 / 当日有pv的用户数；区间同理。无先后约束，不能命名为顺序漏斗转化率。
- 次日复访率：D活跃且D+1活跃人数 / D活跃人数；观察期末日为null，不能当0。首次出现仅指观察期首次，不称新增注册。
- 商品/类目排行：按选定行为计数降序、ID字节序升序；类别直接取事件类别；默认Top20、最大100。事件各维度总和可对账。
- 小时维度为0—23本地小时，按天×小时×行为聚合；无事件桶补0仅在覆盖已确认的日期。

## 训练样本和特征

锚点集合：TRAIN为11月27/28/29/30日；VALIDATION为12月1日；TEST为12月2日；FORECAST为12月4日，全部年份2017、北京时间00:00。历史[t-48h,t)，标签[t,t+24h)。预测人群为历史内有事件的user。FORECAST没有标签，label=null；其他样本y=该标签窗至少一条buy。相同用户可跨日出现，每个(user,t)唯一；不为标签窗新出现但历史为空者生成样本。

每样本保留最后128个历史事件，按(event_ts,source_sha256,source_offset)升序；同秒排序仅为复现，不代表真实发生顺序。左侧截断旧事件，右侧PAD至128；有效长度1—128。序列包括类别桶、行为索引、小时索引、前一保留事件时间差。类别桶=1+int(SHA256(category_id原UTF-8),big_endian)%16384；PAD=0；这是固定哈希特征，无词表泄漏，也不代表类目语义相似。behavior索引pv=1,buy=2,cart=3,fav=4、PAD=0；hour索引hour+1，PAD=0。gap首项为0，其余秒差，截断172800并log1p后除以log1p(172800)。

14维数值特征顺序：按窗口1h、24h、48h分别记录pv/buy/cart/fav次数（12项）；距最近一次事件秒数（1项）；历史48个相对小时桶中非空桶数量（1项）。小时桶按floor((event_ts-(t-48h))/3600)，取0—47。14项均先log1p，使用TRAIN全量均值/总体标准差标准化，标准差为0取1；数值统计计算用float64，输出float32。计数来自完整48h，不受128条截断影响。禁止加入标签窗事件或全数据统计。

序列分片字段：sample_key、user_id、as_of、length、category[128] int32、behavior[128] uint8、hour[128] uint8、gap[128] float32、numeric[14] float32、label int8（FORECAST=-1仅文件编码）、history_event_count int64。sample_key=SHA256(user_id+'|'+as_of UTC秒)。每分片≤32768样本，每列独立.npy，非object dtype；字符串ID单独UTF-8索引文件与int64偏移，禁止pickle。训练索引映射回原ID，不作为模型输入。

## 产物与来源

HDFS根 `/behavior/v1`；raw/source_sha256=.../source.csv；facts/batch_id=.../date=...；aggregates、samples、models、predictions、reports各按batch/run划分。坏行保留source_offset、reason、raw_bytes编码，API不返回原始行。

每bundle必须有manifest.json、config.json、environment.lock引用及_SUCCESS。manifest字段：baseline_version、schema_version、policy_version、batch_id、run_id、stage、code_commit、created_at UTC、inputs[{path,sha256,bytes,rows}]、outputs同结构、config_sha256、environment_sha256、counters、application_id（非YARN任务null）、synthetic、sampled、status。压缩文件hash按原字节；目录记录逐文件hash和排序后的清单hash。哈希不替代语义校验。

来源source.json记录source_url、实际本地路径、文件大小、mtime、sha256、observed_at；downloaded_at未知为null，不用mtime伪装下载时间。无完整官方说明或授权信息时标记provenance_pending，阻止正式对外数据导出，但允许本地研究及实现。不得声称源SHA已实测，实施时补齐。
