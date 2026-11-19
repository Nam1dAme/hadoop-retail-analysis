# 00 全局约定与基线规则

基线 `UBA-DOC-1.0.0`。

## 专项权威与变更原则

专项定义分别以 `04` 数据、`05` 作业、`06` 存储、`07` 模型、`08` 接口、`09` 环境、`11` 验收为准。发现跨文档矛盾时应同步修订，避免静默分歧。

- 业务语义、时间切分、标签、主键或范围变化时升级 major 版本；
- 兼容新增升级 minor 版本；
- 不改变语义的勘误升级 patch 版本。

## 全局约定

- `batch_id`、`run_id`、`experiment_id`、`model_run_id`、`release_id` 均为 UUID 字符串；`attempt` 从 1 开始。
- 任务状态：`PENDING` / `RUNNING` / `SUCCEEDED` / `FAILED` / `CANCELLED`；发布状态：`STAGING` / `READY` / `RETIRED`。
- 协议版本：`schema_version=1.0`、`policy_version=behavior-v1`、`split_version=daily-48h-24h-v1`。
- 业务 ID 作为十进制字符串处理，排序为 UTF-8 字节序；API 不转成 JavaScript 数字。
- 运行审计时间统一 UTC ISO8601（带 `Z`）；业务时间统一 `Asia/Shanghai`。日期范围左闭右开，预测锚点均为北京时间 `00:00`。
- 所有正式产物记录 `baseline`、`schema`、策略版本、输入 hash、参数 hash、代码 commit、环境锁 hash、任务 ID 与输出 hash；全量与抽样、真实与合成、训练与评估严格隔离。
