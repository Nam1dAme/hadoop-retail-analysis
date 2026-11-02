CREATE TABLE data_release (
 release_id CHAR(36) PRIMARY KEY,batch_id CHAR(36) NOT NULL,experiment_id CHAR(36) NOT NULL,
 selected_model_id CHAR(36) NOT NULL,status ENUM('STAGING','READY','RETIRED') NOT NULL,
 manifest_path TEXT NOT NULL,manifest_hash CHAR(64) NOT NULL,manifest_json JSON NOT NULL,
 forecast_as_of DATETIME(6) NOT NULL,forecast_window_end DATETIME(6) NOT NULL,
 loaded_at DATETIME(6) NOT NULL,published_at DATETIME(6) NULL,
 UNIQUE(manifest_hash),FOREIGN KEY(batch_id) REFERENCES data_batch(batch_id),
 FOREIGN KEY(experiment_id) REFERENCES experiment(experiment_id),
 FOREIGN KEY(selected_model_id) REFERENCES model_run(model_run_id)
) COLLATE=utf8mb4_bin;
CREATE TABLE current_release (
 singleton_id TINYINT PRIMARY KEY CHECK(singleton_id=1),release_id CHAR(36) NULL,
 generation BIGINT UNSIGNED NOT NULL,FOREIGN KEY(release_id) REFERENCES data_release(release_id)
) COLLATE=utf8mb4_bin;
INSERT INTO current_release VALUES(1,NULL,0);
CREATE TABLE release_event (
 event_id CHAR(36) PRIMARY KEY,release_id CHAR(36) NOT NULL,
 action ENUM('PUBLISH','ROLLBACK','RETIRE') NOT NULL,previous_release_id CHAR(36) NULL,
 at DATETIME(6) NOT NULL,operator VARCHAR(64) NOT NULL,
 FOREIGN KEY(release_id) REFERENCES data_release(release_id)
) COLLATE=utf8mb4_bin;
CREATE TABLE prediction (
 release_id CHAR(36) NOT NULL,mode ENUM('TEST','FORECAST') NOT NULL,model_run_id CHAR(36) NOT NULL,
 user_id VARCHAR(20) NOT NULL,as_of DATETIME(6) NOT NULL,window_end DATETIME(6) NOT NULL,
 score DOUBLE NOT NULL CHECK(score>=0 AND score<=1),predicted_positive BOOLEAN NOT NULL,
 label TINYINT NULL,history_event_count BIGINT UNSIGNED NOT NULL,sample_key CHAR(64) NOT NULL,
 PRIMARY KEY(release_id,mode,model_run_id,user_id),
 INDEX prediction_scores(release_id,mode,model_run_id,score DESC,user_id ASC),
 CHECK((mode='FORECAST' AND label IS NULL) OR (mode='TEST' AND label IN (0,1))),
 CHECK(window_end>as_of),FOREIGN KEY(release_id) REFERENCES data_release(release_id),
 FOREIGN KEY(model_run_id) REFERENCES model_run(model_run_id)
) COLLATE=utf8mb4_bin;
CREATE TABLE release_load (
 release_id CHAR(36) NOT NULL,dataset VARCHAR(64) NOT NULL,source_hash CHAR(64) NOT NULL,
 rows_loaded BIGINT UNSIGNED NOT NULL,complete BOOLEAN NOT NULL,updated_at DATETIME(6) NOT NULL,
 PRIMARY KEY(release_id,dataset),FOREIGN KEY(release_id) REFERENCES data_release(release_id)
) COLLATE=utf8mb4_bin;
