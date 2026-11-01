# 10 运维、安全、发布与恢复

UBA-DOC-1.0.0 / FROZEN。

## 运行与监控

每任务结构化日志含run_id、stage、batch、时间、级别、error_code、attempt；每10秒更新心跳。记录YARN CPU/GC/memory/vcore seconds、Shuffle/spill、最大与中位task耗时；模型记录epoch loss/AP、学习率、真实样本数、RSS、allocated/reserved及运行时间。日志不含凭据、原始坏行全文或任意用户轨迹。

全局重任务租约见05；不能仅凭陈旧PID或超时杀其他项目进程。取消只作用于已登记的本次application ID或PID+启动时间；确认终态后才能重试。权限不足明确失败，不使用宽泛pkill。

## 发布事务

发布前验证全量数据质量、J01—J06应用证据、三模型评价、GRU真实CUDA训练、CPU/GPU对照、TEST封存和FORECAST评分完整性。每个release绑定同一batch、experiment、selected模型、聚合与预测manifest；缺一不发布正式版本。开发可查看未发布报告，但UI不能冒充正式结果。

先建STAGING并分批装载；检查行数、计数守恒、模型/样本hash、score有限、未来label为NULL；短事务CAS current generation并置READY/写事件。并发发布只有一个胜者，失败者保持STAGING。API按显式release读取，current切换不影响旧请求。

rollback只把current切到另一个完整READY版本，写ROLLBACK事件，不重新计算数据；RETIRED版本不能回滚，必须先验证并恢复为READY（单独管理操作）。重复publish同manifest返回已有release；相同ID不同manifest拒绝。

## 备份和保留

副本1不是备份。备份包含一致性MariaDB快照、release manifest、全部依赖的数据/模型清单及文件hash、环境锁、配置脱敏副本。备份到已核实的独立物理介质或卷；同VHDX另一目录只能称本地副本。独立备份条件未满足必须标明可靠性门禁未通过。

至少保留current、上一个READY版本、所有被保留版本引用的源/事实/特征/模型；失败临时目录7天，任务日志30天。删除前计算引用闭包并dry-run列表，不能删除活动任务或被引用产物；没有已验证备份不得回收唯一源归档。空间紧张停止新任务，不突破引用保护。

恢复演练在独立DB/HDFS根和端口执行：恢复快照→逐文件hash→重建结果或恢复结果库→同release查询与预测对账→确认正式current未变化。演练RPO为最后成功发布版本，RTO目标≤4小时；实测记录而非承诺已达成。

## 安全

只监听localhost，日志/manifest脱敏；源码与文档不保存密钥，凭据由受限权限文件注入。API只读账号不能INSERT/UPDATE/DDL或读服务器文件。SQL参数化、ID格式校验、分页上限、游标签名、路径白名单。报告接口不暴露原始文件路径、训练checkpoint下载或原始用户行为。

数据中的匿名ID仍按研究数据最小化处理，不尝试反查真实用户。只加载可信本项目模型产物；CPU可加载state_dict用于验证不代表允许加载陌生pickle。明确来源授权前不对外分发原始数据。
