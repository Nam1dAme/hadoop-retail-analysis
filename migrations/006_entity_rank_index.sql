ALTER TABLE entity_daily
 ADD INDEX entity_rank_daily(release_id,entity_type,behavior_type,date,event_count DESC,entity_id ASC),
 ALGORITHM=INPLACE, LOCK=NONE;
