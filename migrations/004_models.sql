CREATE TABLE model_run (
 model_run_id CHAR(36) PRIMARY KEY, experiment_id CHAR(36) NOT NULL,
 algorithm ENUM('CONSTANT','LOGISTIC','GRU') NOT NULL, status VARCHAR(16) NOT NULL,
 seed INT NOT NULL, config_hash CHAR(64) NOT NULL, config_json JSON NOT NULL,
 device VARCHAR(32) NOT NULL, checkpoint_path TEXT NULL, checkpoint_hash CHAR(64) NULL,
 resume_path TEXT NULL, resume_hash CHAR(64) NULL,
 parameter_count BIGINT UNSIGNED NOT NULL, best_epoch INT NULL, threshold DOUBLE NULL,
 hardware_json JSON NOT NULL, training_seconds DOUBLE NULL,
 peak_allocated_mib DOUBLE NULL, peak_reserved_mib DOUBLE NULL, rss_mib DOUBLE NULL,
 created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL,
 UNIQUE(experiment_id,algorithm,config_hash), FOREIGN KEY(experiment_id) REFERENCES experiment(experiment_id)
) COLLATE=utf8mb4_bin;
CREATE TABLE model_epoch (
 model_run_id CHAR(36) NOT NULL, epoch INT NOT NULL, train_loss DOUBLE NOT NULL,
 validation_ap DOUBLE NULL, validation_logloss DOUBLE NOT NULL,
 duration_seconds DOUBLE NOT NULL, sample_count BIGINT UNSIGNED NOT NULL,
 peak_gpu_mib DOUBLE NOT NULL, skipped_updates INT UNSIGNED NOT NULL,
 PRIMARY KEY(model_run_id,epoch), FOREIGN KEY(model_run_id) REFERENCES model_run(model_run_id)
) COLLATE=utf8mb4_bin;
CREATE TABLE model_metric (
 model_run_id CHAR(36) NOT NULL, split ENUM('VALIDATION','TEST') NOT NULL,
 cohort ENUM('ALL','SEEN_TRAIN','UNSEEN_TRAIN') NOT NULL, name VARCHAR(32) NOT NULL,
 value DOUBLE NULL, sample_count BIGINT UNSIGNED NOT NULL, positive_count BIGINT UNSIGNED NOT NULL,
 reason VARCHAR(64) NULL, evaluation_hash CHAR(64) NOT NULL,
 PRIMARY KEY(model_run_id,split,cohort,name), FOREIGN KEY(model_run_id) REFERENCES model_run(model_run_id)
) COLLATE=utf8mb4_bin;
CREATE TABLE benchmark (
 benchmark_id CHAR(36) PRIMARY KEY, experiment_id CHAR(36) NOT NULL, model_run_id CHAR(36) NULL,
 device VARCHAR(32) NOT NULL, `precision` VARCHAR(16) NOT NULL, repeat_index INT NOT NULL,
 sample_hash CHAR(64) NOT NULL, config_hash CHAR(64) NOT NULL, steps INT NOT NULL,
 elapsed_seconds DOUBLE NOT NULL, end_to_end_seconds DOUBLE NOT NULL, throughput DOUBLE NOT NULL,
 peak_allocated_mib DOUBLE NULL, peak_reserved_mib DOUBLE NULL, rss_mib DOUBLE NOT NULL,
 hardware_json JSON NOT NULL, FOREIGN KEY(experiment_id) REFERENCES experiment(experiment_id)
) COLLATE=utf8mb4_bin;
