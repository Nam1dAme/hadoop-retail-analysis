CREATE INDEX IF NOT EXISTS task_run_batch_created_run ON task_run(batch_id, created_at, run_id);
CREATE INDEX IF NOT EXISTS data_release_status_published_release ON data_release(status, published_at, release_id);
CREATE INDEX IF NOT EXISTS benchmark_experiment_device_repeat ON benchmark(experiment_id, device, repeat_index);
