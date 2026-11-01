# 05 Hadoop作业与编排

UBA-DOC-1.0.0 / FROZEN。以下是未来实现协议，不表示命令已经存在。

## DAG与物理作业

| 阶段 | 输入→输出 | 键、状态和规模约束 |
|---|---|---|
| I00 | CSV→raw清单 | 流式SHA和归档；同hash+policy复用batch；不复制到旧项目目录 |
| J01 | raw→facts/rejected/quality | Map解析、统计，来源偏移保持；按日期输出；真正YARN应用 |
| J02 | facts→profile | 五字段键归组计重复候选；分别按user/item/category去重计基数，多作业子ID独立登记；不在driver建集合 |
| J03 | scope facts→event_cube/entity_daily | 日/小时/type及日/entity/type键计数；Combiner仅加法；热点键可先按source_offset hash盐分16路，再去盐汇总 |
| J04 | scope facts→user_daily/user_masks/range_summary | user,date二级排序，单用户流式计四行为与小时位图；9日bitmask计算45种连续日期范围的UV与pv∩buy；另算日首次出现与D/D+1复访 |
| J05 | scope facts→raw_samples | partition=user；sort=(user,ts,source,offset)；group=user；按固定7个锚点维护有界状态，输出TRAIN4+VAL1+TEST1+FORECAST1 |
| J06 | TRAIN raw_samples→normalization | 各14维sum/sumsq/count双精度合并，方差负的舍入误差钳0；不读VAL/TEST统计 |
| E01 | raw_samples+normalization→local shards+HDFS archive | 流式标准化与固定形状分片；TRAIN/VAL/TEST/FORECAST分目录，输出完整行数和样本hash |
| M01 | shards→三模型/评价 | CLI单GPU任务，CUDA证据和07门禁 |
| P01 | FORECAST shards→predictions | 选定评估模型原权重批量推断，真实GPU或明确CPU推断记录；不重训全历史模型 |
| L01 | aggregates+predictions→STAGING release | 分批入库，逐表对账、约束与hash验证 |
| R01 | STAGING→READY/current | 短事务发布；必须关联完整模型与真实GPU训练证据 |

J02有重复、三类distinct、item-category一致性等物理子作业；J03/J04也允许拆成多作业。每个子作业登记自己的application ID、输入输出和计数，逻辑阶段仅在全部成功时完成。不得用一个虚构application ID覆盖整个DAG。

J04日期位图只覆盖固定9天。对每个用户、每个连续区间判断active/pv/buy位图交集并输出计数；pv与buy可发生在区间内不同天。次日复访的分子来自active位图相邻位，不由日UV相减。range_summary共45个区间，支持API任意整日范围准确UV。entity_daily全量保存商品/类目每日四行为数；不对每个商品维护全用户集合。

J05每用户每锚点维护14维统计、48位活跃桶、最后事件时间及最多128条序列；标签只需buy存在标记。整个用户原始事件不得list化。SEEN_TRAIN集合由TRAIN样本user_id去重产生，按user做外排join给验证/测试样本标记cohort；不得在driver加载全部用户集合。类别碰撞统计使用(category_id,bucket)去重后按bucket聚合，记录不同类目数超过1的桶数，不把碰撞数据改变为词表输入。单用户超大历史仍为流式，长尾靠监测，不把一个用户随意拆开导致错误标签。无需构造全量购物篮、共现对或用户×商品矩阵。

## 调度与资源

默认Reducer=2，可配置1—64且记录实际值；没有旧的1—4硬限制。根据实际输入量和倾斜试验选择；更多Reducer不保证单机更快。重任务串行，AM与容器预算见09。各输入必须为前置成功manifest指向的确切文件，拒绝glob误读历史版本。

原始文件分片不依赖业务日期排序；事实按日期分区、每阶段目标文件128—256MiB，尾文件可小。不得按用户/商品建海量目录；输出超过10000 part文件阻止下一阶段并要求合并规划。正式验算使用独立MR计数与小样例Python计算，禁止逐阶段把全量数据拉回SQLite重算。

## 状态、失败和幂等

调度器持有DB命名锁与带fencing generation的资源租约，每10秒心跳；任何提交、提交结果登记及发布都核对generation。PID+启动时间、YARN终态及精确任务标识共同判断存活，不能仅因心跳过期重跑。租约不确定时停止新提交并保留状态，恢复者先核查旧进程/应用。

每次尝试新run_id；同输入+参数+代码+环境hash成功结果只校验复用，不覆盖。失败保留临时目录7天。已经rename但未登记时核对manifest补录；DB成功而文件丢失则失败并报告，不无声生成不同产物。YARN最多自动重试同任务两次，业务解析错误不重试；调度层失败由显式resume续跑。

## CLI契约

规划入口 `behaviorctl`；子命令profile、ingest、pipeline、prepare-samples、train、evaluate、predict、publish、status、resume、rollback、backup、restore、benchmark。统一参数--config、--baseline、--batch-id（来源注册前除外）、--dry-run；涉及模型传experiment/model ID，涉及发布传release ID。dry-run只验证依赖和打印计划，不注册任务、不写产物。

退出码0成功/已存在可复用，2参数或契约错误，3依赖不可用，4质量失败，5资源或任务失败，6锁冲突。输出JSON包含status、IDs、manifest_path、error_code；日志写文件不混入JSON。正式配置与合成测试配置的DB/HDFS/local根必须三重隔离。
