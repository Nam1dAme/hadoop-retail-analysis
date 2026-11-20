ALTER TABLE prediction
 DROP PRIMARY KEY,
 ADD PRIMARY KEY(release_id,mode,model_run_id,user_id,as_of),
 DROP INDEX prediction_scores,
 ADD INDEX prediction_scores(release_id,mode,model_run_id,score DESC,user_id ASC,as_of ASC);
