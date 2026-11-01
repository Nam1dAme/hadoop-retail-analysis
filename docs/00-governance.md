# 00 文档治理与冻结规则

基线 UBA-DOC-1.0.0；FROZEN；2026-11-02。

## 权威与变更

用户新指令优先；专项定义分别以04数据、05作业、06存储、07模型、08接口、09环境、11验收为准。发现跨文档矛盾必须统一修订，不能静默选一种实现。原仓库HL-DOC基线仅为参考，不约束新业务。

设计冻结不等于软件完成。性能阈值是验收目标，不是实测结论；硬件型号由用户提供，可用性由后续环境门禁证明。数据来源的官方说明、下载凭证与全量画像尚需补证，不编造下载时间或数据统计。

业务语义、时间切分、标签、主键或范围变化升major；兼容新增升minor；不改变语义的勘误升patch。变更记录CR-ID、日期、用户指令或理由、受影响需求与测试、迁移及回滚；重跑链接、术语、一致性及哈希审查。用户已授权的变更无需再次索要形式许可。

执行记录放后续 `docs/execution/`，不属于本次冻结集合；依赖锁与运行配置在实现阶段创建。不得用更新哈希掩盖未经记录的语义变化。

## 全局约定

batch_id、run_id、experiment_id、model_run_id、release_id均为UUID字符串；attempt从1开始。任务状态为PENDING/RUNNING/SUCCEEDED/FAILED/CANCELLED，发布状态为STAGING/READY/RETIRED。schema_version=1.0；policy_version=behavior-v1；split_version=daily-48h-24h-v1。

业务ID作为十进制字符串处理，排序为UTF-8字节序；API不转成JavaScript数字。运行审计时间统一UTC ISO8601带Z；业务时间统一Asia/Shanghai。日期范围左闭右开；文档中的预测锚点均为北京时间00:00。

所有正式产物记录baseline、schema、策略版本、输入hash、参数hash、代码commit、环境锁hash、任务ID和输出hash；没有代码commit只能为开发试跑。全量与抽样、真实与合成、训练与评估不得混淆。

## 当前范围

本轮仅19份Markdown（含README、00—16和完整性清单）。不安装依赖、不启动服务、不写业务代码。实施代理不得因本文标题自行启动多代理；默认单代理按12执行。未明确要求的外部发布或消息发送不属于本设计交付。
