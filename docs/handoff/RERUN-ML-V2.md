# 样本 v2 / 模型协议重跑手册

- 适用分支：`fix/ml-evaluation-v2`。
- 当前进度见 [HANDOFF-2026-11-20.md](HANDOFF-2026-11-20.md)。

## 执行身份

必须以 OS 用户 `hadoop` 执行：

- HDFS 数据目录归 `hadoop` 所有，DataNode 要求目录属主与运行用户一致。
- MariaDB 管理员使用 socket 认证，同样绑定 `hadoop`。

在其他账号下可以这样调用：`sudo -u hadoop -H bash -lc '...'`。前提是 `/etc/sudoers.d/behavior-rerun` 已授权。

## 前置条件

- 训练与发布脚本要求 `git status --porcelain` 为空，执行前不要留下未提交的改动。
- **Pipeline 运行期间不要修改 `orchestration/`、`ingest/`、`ml/` 下的 `*.py`**：`implementation_fingerprint` 覆盖这些文件，源码一变，正在运行的 pipeline 会以 `Runtime source changed during pipeline` 中止。
- 修改 `docs/` 不受影响。

## 为什么必须全量重跑

- `implementation_fingerprint` 和 `jar_sha256` 都已变化，所以 J01–J06 会以新身份重新执行，不会复用旧产物。
- 旧版 1.0（14 维）样本缓存会被 `ShardDataset` 明确拒绝。

## 步骤

```bash
cd /home/hadoop/hadoop-retail-analysis
git switch fix/ml-evaluation-v2
source scripts/dev-env.sh
python scripts/database-service.py start
scripts/hadoop-service.sh start
B=1c577aba-46f2-4710-b296-5f29ac842921

# 1. 迁移，先备份。必须用 migrator 凭据；默认的 writer 没有 CREATE 权限
scripts/behaviorctl --baseline UBA-DOC-1.0.0 --credentials var/secrets/migrator.cnf migrate --backup

# 2. J01..J06 + Q01/A01 审计
#    已成功的阶段会按身份复用。中断后重新执行同一命令即可续跑
scripts/behaviorctl --baseline UBA-DOC-1.0.0 pipeline --batch-id $B --through J06

# 3. 合并 GPU 吞吐优化分支，必须在步骤 2 完成之后、步骤 4 之前
#    原因：它修改了 ml/*.py（会中止运行中的 pipeline），且 E01 导出会用到它改过的 ml/shards.py
git merge --no-ff perf/gpu-throughput
.venv/bin/python -m pytest -q

# 4. E01 导出 v2 分片；成功时才替换 samples.json
scripts/behaviorctl --baseline UBA-DOC-1.0.0 prepare-samples --batch-id $B > /tmp/samples.json \
  && grep -q '"status": *"SUCCEEDED"' /tmp/samples.json && mv /tmp/samples.json var/production/samples.json

# 5. 训练与评估
#    候选：CONSTANT/RULE/GBDT/LOGISTIC/GRU × 2 lr，外加 seed 43/44
#    合并 GPU 分支后 GRU 每 epoch 约 3 分钟，总计约 2–3 小时
.venv/bin/python scripts/train-models.py
.venv/bin/python scripts/verify-models.py

# 6. 预测与发布：生成新 release，旧 release 保留，可回滚
.venv/bin/python scripts/publish-results.py forecast
.venv/bin/python scripts/publish-results.py publish
.venv/bin/python scripts/verify-release.py

# 7. 发布后验收（需先执行 python scripts/web-service.py start）
.venv/bin/python scripts/verify-api.py
.venv/bin/python scripts/verify-entity-ranking.py
.venv/bin/python scripts/verify-model-page.py
.venv/bin/python scripts/audit-delivery.py
```

## 验收要点

### `var/production/samples.json`

- `split_version` 为 `daily-48h-24h-v2`。
- TEST 行数约为旧 TEST（88.9 万）加上 12/03 锚点的样本数。

### `var/production/models.json`

- `seed_variance.LOGISTIC` 与 `seed_variance.GRU` 的 `seeds` 均为 `[42,43,44]`。
- `test[*].metrics` 包含：
  - `DAY:2017-12-02`、`DAY:2017-12-03` 两个按天分组；
  - `precision_at_1pct` 等 top-k 指标。

### 模型对比

比较 RULE、GBDT 与 GRU 的 TEST AP 和 lift@1%：

- 如果 GRU 相对 GBDT 的提升在种子标准差之内，说明序列建模没有可信增益，收益来自特征。
- 如果 GBDT 明显优于 RULE，说明数值特征之间的交互有价值。

### 回滚

```bash
.venv/bin/python scripts/publish-results.py rollback \
  --release-id b6091bad-3176-4297-a48f-218f05e981e1 \
  --expected-generation <当前 generation>
```
