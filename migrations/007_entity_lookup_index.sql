ALTER TABLE entity_daily
 DROP INDEX release_id,
 ADD INDEX entity_lookup(release_id,entity_type,entity_id,behavior_type,date,event_count),
 ALGORITHM=INPLACE, LOCK=NONE;
