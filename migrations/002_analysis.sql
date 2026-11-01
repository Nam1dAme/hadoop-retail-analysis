CREATE TABLE event_cube (
 release_id CHAR(36) NOT NULL, date DATE NOT NULL, hour TINYINT UNSIGNED NOT NULL CHECK(hour<24),
 behavior_type ENUM('pv','buy','cart','fav') NOT NULL, event_count BIGINT UNSIGNED NOT NULL,
 PRIMARY KEY(release_id,date,hour,behavior_type)
) COLLATE=utf8mb4_bin;
CREATE TABLE entity_daily (
 release_id CHAR(36) NOT NULL, date DATE NOT NULL, entity_type ENUM('ITEM','CATEGORY') NOT NULL,
 entity_id VARCHAR(20) NOT NULL, behavior_type ENUM('pv','buy','cart','fav') NOT NULL,
 event_count BIGINT UNSIGNED NOT NULL, PRIMARY KEY(release_id,date,entity_type,entity_id,behavior_type),
 INDEX(release_id,entity_type,behavior_type,date,entity_id)
) COLLATE=utf8mb4_bin;
CREATE TABLE user_daily (
 release_id CHAR(36) NOT NULL, user_id VARCHAR(20) NOT NULL, date DATE NOT NULL,
 pv_count BIGINT UNSIGNED NOT NULL, buy_count BIGINT UNSIGNED NOT NULL,
 cart_count BIGINT UNSIGNED NOT NULL, fav_count BIGINT UNSIGNED NOT NULL,
 first_ts BIGINT NOT NULL, last_ts BIGINT NOT NULL, active_hours TINYINT UNSIGNED NOT NULL,
 PRIMARY KEY(release_id,user_id,date), INDEX(release_id,date,user_id)
) COLLATE=utf8mb4_bin;
CREATE TABLE range_summary (
 release_id CHAR(36) NOT NULL, start_date DATE NOT NULL, end_date DATE NOT NULL,
 uv BIGINT UNSIGNED NOT NULL,pv_users BIGINT UNSIGNED NOT NULL,pv_buy_users BIGINT UNSIGNED NOT NULL,
 event_count BIGINT UNSIGNED NOT NULL, PRIMARY KEY(release_id,start_date,end_date),
 CHECK(end_date>start_date), CHECK(pv_buy_users<=pv_users AND pv_users<=uv)
) COLLATE=utf8mb4_bin;
CREATE TABLE activity_daily (
 release_id CHAR(36) NOT NULL,date DATE NOT NULL,active_users BIGINT UNSIGNED NOT NULL,
 first_observed_users BIGINT UNSIGNED NOT NULL,return_next_users BIGINT UNSIGNED NULL,
 PRIMARY KEY(release_id,date),CHECK(first_observed_users<=active_users),CHECK(return_next_users<=active_users)
) COLLATE=utf8mb4_bin;
