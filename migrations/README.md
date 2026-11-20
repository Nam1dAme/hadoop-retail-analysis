# 数据库迁移说明（001—010）

使用本项目 migrator 账号运行迁移：

```bash
scripts/behaviorctl --baseline UBA-DOC-1.0.0 --credentials var/secrets/migrator.cnf migrate
```

迁移前应先使用 `mariadb-dump` 备份至对应工作目录。`schema_migration` 表记录各迁移文件的 SHA256，**已应用的迁移文件不得修改**；若 DDL 中断导致部分迁移，须核对备份与实际表结构后再处理，不可静默假装已回滚。

## 迁移清单

- **001_control.sql**：建立批次与任务控制表（`data_batch`、`task_run`、`resource_lease`、`quality_metric`）。
- **002_analysis.sql**：建立零售行为分析结果表（`event_cube`、`entity_daily`、`user_daily`、`range_summary`、`activity_daily`）。
- **003_samples.sql**：建立预测实验元数据表（`experiment`）。
- **004_models.sql**：建立模型训练、周期指标与基准测试表（`model_run`、`model_epoch`、`model_metric`、`benchmark`）。
- **005_release.sql**：建立数据发布与预测结果表（`data_release`、`current_release`、`release_event`、`prediction`、`release_load`）。
- **006_entity_rank_index.sql**：为 `entity_daily` 增加按日排行复合索引 `entity_rank_daily(release_id, entity_type, behavior_type, date, event_count DESC, entity_id ASC)`。
- **007_entity_lookup_index.sql**：替换 `entity_daily` 冗余索引，新增实体趋势查询索引 `entity_lookup(release_id, entity_type, entity_id, behavior_type, date, event_count)`。
- **008_api_query_indexes.sql**：为 API 常用查询路径补充复合索引：`task_run(batch_id, created_at, run_id)`、`data_release(status, published_at, release_id)` 与 `benchmark(experiment_id, device, repeat_index)`（使用 `CREATE INDEX IF NOT EXISTS`）。
- **009_model_protocol.sql**：扩展 `model_metric.cohort` 与 `model_run.algorithm` 字段，纳入 `RULE` 基线。
- **010_gbdt.sql**：将 `model_run.algorithm` 的 ENUM 扩展为包含 `GBDT` 直方图梯度提升基线。

## 时间与时区语义

- **`DATE` 与 `hour` 列**（如 `event_cube.date`、`event_cube.hour`、`entity_daily.date`、`user_daily.date`、`range_summary.start_date/end_date`、`activity_daily.date`）：按业务时区 **`Asia/Shanghai`（UTC+8）** 切分的自然日与小时（0–23）。
- **`DATETIME(6)` 列**（如 `created_at`、`updated_at`、`published_at`、`forecast_as_of`、`forecast_window_end`、`as_of`、`window_end`、`heartbeat_at` 等）：统一存储 **UTC** 时间（由 `UTC_TIMESTAMP(6)` 或应用层转换后写入），API 返回时格式化为 ISO-8601 UTC 字符串（末尾带 `Z`）。
- **`BIGINT` 时间戳列**（如 `user_daily.first_ts`、`user_daily.last_ts`）：存储 **UNIX 秒级时间戳**（epoch seconds），与时区无关，API 层按 UTC 转为 ISO-8601 `Z` 格式返回。
