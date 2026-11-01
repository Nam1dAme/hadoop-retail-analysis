CREATE TABLE experiment (
 experiment_id CHAR(36) PRIMARY KEY,batch_id CHAR(36) NOT NULL,split_version VARCHAR(64) NOT NULL,
 sample_manifest TEXT NOT NULL,sample_hash CHAR(64) NOT NULL,normalization_hash CHAR(64) NOT NULL,
 status VARCHAR(16) NOT NULL,selected_model_id CHAR(36) NULL,test_sealed_at DATETIME(6) NULL,
 selection_json JSON NOT NULL,created_at DATETIME(6) NOT NULL,updated_at DATETIME(6) NOT NULL,
 UNIQUE(batch_id,sample_hash),FOREIGN KEY(batch_id) REFERENCES data_batch(batch_id)
) COLLATE=utf8mb4_bin;
