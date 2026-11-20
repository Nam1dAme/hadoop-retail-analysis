# 样本 v2 / 模型协议重跑手册

适用分支：`fix/ml-evaluation-v2`。

必须以 OS 用户 `hadoop` 执行。原因有两个：

- HDFS 数据目录归 `hadoop` 所有，DataNode 要求目录属主与运行用户一致。
- MariaDB 管理员使用 socket 认证，同样绑定 `hadoop` 用户。

训练与发布脚本要求 `git status --porcelain` 为空，执行前请勿留下未提交改动。

## 为什么必须全量重跑

- `implementation_fingerprint` 覆盖了 `orchestration`、`ingest`、`ml` 下的源码。
- `jar_sha256` 已变化。
- 因此 J01–J06 都会以新身份重新执行，不会复用旧产物。
- 旧版 1.0（14 维）样本缓存会被 `ShardDataset` 明确拒绝。

## 步骤

```bash
cd /home/hadoop/hadoop-retail-analysis
git switch fix/ml-evaluation-v2
source scripts/dev-env.sh
python scripts/database-service.py start
scripts/hadoop-service.sh start
B=1c577aba-46f2-4710-b296-5f29ac842921

# 1. 迁移（009：cohort 改 VARCHAR，algorithm 增加 RULE），先备份
scripts/behaviorctl --baseline UBA-DOC-1.0.0 migrate --backup

# 2. J01..J06 + Q01/A01 审计，旧耗时约 1.5–2 小时
scripts/behaviorctl --baseline UBA-DOC-1.0.0 pipeline --batch-id $B --through J06

# 3. E01 导出 v2 分片；仅在成功时替换 samples.json
scripts/behaviorctl --baseline UBA-DOC-1.0.0 prepare-samples --batch-id $B > /tmp/samples.json \
  && grep -q '"status": *"SUCCEEDED"' /tmp/samples.json && mv /tmp/samples.json var/production/samples.json

# 4. 训练与评估，预计 4–6 小时（RULE/CONSTANT/LOGISTIC/GRU × 2 lr，外加 seed 43/44）
.venv/bin/python scripts/train-models.py
.venv/bin/python scripts/verify-models.py

# 5. 预测与发布（新 release，旧 release 保留可回滚）
.venv/bin/python scripts/publish-results.py forecast
.venv/bin/python scripts/publish-results.py publish
.venv/bin/python scripts/verify-release.py
```

## 验收要点

- `var/production/samples.json`：
  - `split_version` 为 `daily-48h-24h-v2`。
  - TEST 行数约为旧 TEST（88.9 万）加上 12/03 锚点的样本数。
- `var/production/models.json`：
  - `seed_variance.LOGISTIC/GRU.seeds == [42,43,44]`。
  - `test[*].metrics` 中包含 `DAY:2017-12-02`、`DAY:2017-12-03` 两个分组，以及 `precision_at_1pct` 等 top-k 指标。
- 对比 RULE 与 GRU 的 TEST AP 和 lift@1%：
  - 如果 GRU 的提升在种子标准差之内，说明序列模型相对规则没有可信增益。
- 回滚：`publish-results.py rollback --release-id b6091bad-3176-4297-a48f-218f05e981e1 --expected-generation <当前 generation>`。
