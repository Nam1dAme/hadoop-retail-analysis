# 06 数据库逻辑设计

UBA-DOC-1.0.0 / FROZEN。只定义结构，不提供本轮DDL。

MariaDB数据库名 `behavior_analytics`；UTF-8、业务ID二进制排序。UUID用CHAR(36)，SHA用CHAR(64)，源ID用VARCHAR(20)，时间用UTC DATETIME(6)，日期DATE，计数BIGINT非负，分数DOUBLE且有限。JSON结构由应用契约验证。下面未注明可空的字段均NOT NULL；created_at/updated_at为控制表公共字段，分析表以release_id溯源不逐行重复审计时间。

| 表 / 主键 | 必需字段（除主键外）及约束 |
|---|---|
| data_batch / batch_id | source_path、source_url可空、source_sha256、source_bytes、source_mtime、observed_at、downloaded_at可空、policy_version、baseline_version、status、raw_rows/accepted_rows/rejected_rows/in_scope_rows/outside_rows（未完成可空）、manifest_path可空；UNIQUE(source_sha256,policy_version) |
| task_run / run_id | batch_id、stage、attempt、parent_run_id可空、status、input_hash、config_hash、code_commit、environment_hash、application_id可空、pid可空、process_started_at可空、lease_generation、heartbeat_at可空、started_at/finished_at可空、manifest_path/error_code可空；UNIQUE(batch_id,stage,input_hash,config_hash,code_commit,environment_hash,attempt) |
| resource_lease / singleton_id=1 | generation、owner_run_id可空、heartbeat_at可空；仅调度writer更新 |
| quality_metric / (batch_id,name,dimension) | value BIGINT、detail_json；dimension为空串表示全局；记录重复候选/原因计数/每日覆盖 |
| experiment / experiment_id | batch_id、split_version、sample_manifest、sample_hash、normalization_hash、status、selected_model_id可空、test_sealed_at可空、selection_json |
| model_run / model_run_id | experiment_id、algorithm(CONSTANT/LOGISTIC/GRU)、status、seed、config_json、device、checkpoint_path可空、checkpoint_hash可空、parameter_count、best_epoch可空、threshold可空、hardware_json、training_seconds可空、peak_allocated_mib/peak_reserved_mib/rss_mib可空 |
| model_epoch / (model_run_id,epoch) | train_loss、validation_ap可空、validation_logloss、duration_seconds、sample_count、peak_gpu_mib |
| model_metric / (model_run_id,split,cohort,name) | value可空、sample_count、positive_count、reason可空、evaluation_hash；split=VALIDATION/TEST；cohort=ALL/SEEN_TRAIN/UNSEEN_TRAIN |
| benchmark / benchmark_id | experiment_id、model_run_id、device、precision、repeat_index、sample_hash、config_hash、steps、elapsed_seconds、end_to_end_seconds、throughput、peak_allocated_mib/peak_reserved_mib可空、rss_mib、hardware_json |
| data_release / release_id | batch_id、experiment_id、selected_model_id、status、manifest_path、manifest_hash、forecast_as_of、forecast_window_end、loaded_at、published_at可空；所有关联必须同batch |
| current_release / singleton_id=1 | release_id、generation；外键仅指READY |
| release_event / event_id UUID | release_id、action(PUBLISH/ROLLBACK/RETIRE)、previous_release_id可空、at、operator |
| event_cube / (release_id,date,hour,behavior_type) | event_count；hour 0—23 |
| entity_daily / (release_id,date,entity_type,entity_id,behavior_type) | event_count；entity_type=ITEM/CATEGORY |
| user_daily / (release_id,user_id,date) | pv_count、buy_count、cart_count、fav_count、first_ts、last_ts、active_hours；保留供精确用户页与校验，不含原始事件 |
| range_summary / (release_id,start_date,end_date) | uv、pv_users、pv_buy_users、event_count；end_date不含；pv_buy_users≤pv_users≤uv |
| activity_daily / (release_id,date) | active_users、first_observed_users、return_next_users可空；末日return_next_users=NULL |
| prediction / (release_id,mode,model_run_id,user_id) | as_of、window_end、score、predicted_positive、label可空、history_event_count、sample_key；mode=TEST/FORECAST；TEST存三模型，FORECAST仅selected |

FORECAST label必须NULL；TEST label只能0/1；score必须[0,1]且有限；as_of/window_end与mode契约一致。所有事实结果关联READY/STAGING版本，不允许跨release直接联表。数据库CHECK和应用装载校验双重执行。

索引：task_run(status,heartbeat_at)，model_run(experiment_id,algorithm)，entity_daily(release_id,entity_type,behavior_type,date,entity_id)，user_daily(release_id,date,user_id)，prediction(release_id,mode,model_run_id,score DESC,user_id ASC)。预测明细精确用户查询使用主键，游标按score与user_id组合；禁止深OFFSET。排行跨日需要GROUP BY后TopK，性能未达标可增加同口径范围排行物化表，不能改变排序或只聚合单日TopK。

装载每1000行一个事务，单包≤8MiB；失败版本仍STAGING，可按release_id清理或幂等续装。完成后逐表count及关键总计校验，再单事务CAS current generation、写release_event、置READY。不能用超大事务装载全部预测。

控制writer、结果loader、API reader独立账号；reader无写和文件权限。迁移起点001，新项目独立库，不迁移旧销售事实；结构迁移先备份、记录版本及hash，回滚方案不得自动删除旧项目数据库。
