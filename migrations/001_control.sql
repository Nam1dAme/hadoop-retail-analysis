CREATE TABLE data_batch (
 batch_id CHAR(36) PRIMARY KEY, source_path TEXT NOT NULL, source_url TEXT NULL,
 source_sha256 CHAR(64) NOT NULL, source_bytes BIGINT UNSIGNED NOT NULL,
 source_mtime DATETIME(6) NOT NULL, observed_at DATETIME(6) NOT NULL, downloaded_at DATETIME(6) NULL,
 policy_version VARCHAR(64) NOT NULL, baseline_version VARCHAR(64) NOT NULL,
 status VARCHAR(16) NOT NULL, raw_rows BIGINT UNSIGNED NULL, accepted_rows BIGINT UNSIGNED NULL,
 rejected_rows BIGINT UNSIGNED NULL, in_scope_rows BIGINT UNSIGNED NULL, outside_rows BIGINT UNSIGNED NULL,
 manifest_path TEXT NULL, source_metadata JSON NOT NULL,
 created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL,
 UNIQUE(source_sha256,policy_version)
) COLLATE=utf8mb4_bin;
CREATE TABLE task_run (
 run_id CHAR(36) PRIMARY KEY, batch_id CHAR(36) NOT NULL, stage VARCHAR(40) NOT NULL,
 attempt INT UNSIGNED NOT NULL, parent_run_id CHAR(36) NULL, status VARCHAR(16) NOT NULL,
 input_hash CHAR(64) NOT NULL, config_hash CHAR(64) NOT NULL, code_commit VARCHAR(64) NOT NULL,
 environment_hash CHAR(64) NOT NULL, application_id VARCHAR(80) NULL,
 pid BIGINT NULL, process_started_at VARCHAR(64) NULL, lease_generation BIGINT UNSIGNED NOT NULL,
 heartbeat_at DATETIME(6) NULL, started_at DATETIME(6) NULL, finished_at DATETIME(6) NULL,
 manifest_path TEXT NULL, error_code VARCHAR(64) NULL, parameters JSON NOT NULL,
 output_path TEXT NOT NULL, created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL,
 UNIQUE(batch_id,stage,input_hash,config_hash,code_commit,environment_hash,attempt),
 INDEX(status,heartbeat_at), FOREIGN KEY(batch_id) REFERENCES data_batch(batch_id)
) COLLATE=utf8mb4_bin;
CREATE TABLE resource_lease (
 singleton_id INT PRIMARY KEY CHECK(singleton_id=1), generation BIGINT UNSIGNED NOT NULL,
 owner_run_id CHAR(36) NULL, heartbeat_at DATETIME(6) NULL,
 created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL
) COLLATE=utf8mb4_bin;
INSERT INTO resource_lease VALUES(1,0,NULL,NULL,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6));
CREATE TABLE quality_metric (
 batch_id CHAR(36) NOT NULL, name VARCHAR(64) NOT NULL, dimension VARCHAR(64) NOT NULL,
 value BIGINT NOT NULL, detail_json JSON NOT NULL,
 PRIMARY KEY(batch_id,name,dimension), FOREIGN KEY(batch_id) REFERENCES data_batch(batch_id)
) COLLATE=utf8mb4_bin;
