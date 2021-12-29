DELIMITER $$
CREATE PROCEDURE `GET_TOP_COMPOSERS`(
	minimum_songs INT,
    num_composers_per_service INT
)
BEGIN
	SELECT source_rank, pop_measure_type_id, composer_name, avg_popularity, num_songs FROM 
	(
		SELECT *, ROW_NUMBER() OVER (
			PARTITION BY pop_measure_type_id
			ORDER BY avg_popularity DESC
		) AS source_rank FROM 
		(
			SELECT composer_name, pop_measure_type_id, ROUND(AVG(value)) AS avg_popularity, COUNT(DISTINCT song_id) AS num_songs 
			FROM classically.composer 
			JOIN song USING (composer_id)
			JOIN recording USING (song_id)
			JOIN popularity_measure USING (recording_id)
			GROUP BY composer_id, pop_measure_type_id
			HAVING num_songs > minimum_songs
			ORDER BY pop_measure_type_id, avg_popularity DESC
		) x
		ORDER BY pop_measure_type_id, avg_popularity DESC, source_rank ASC
	) y
	WHERE source_rank <= num_composers_per_service; 
END$$
DELIMITER ;

DELIMITER $$
CREATE PROCEDURE `GET_TOP_PERFORMERS`(
	minimum_recordings INT,
    num_performers_per_service INT
)
BEGIN
	SELECT source_rank, pop_measure_type_id, performer_name, avg_popularity, num_recordings 
    FROM (
		SELECT *, ROW_NUMBER() OVER (
			PARTITION BY pop_measure_type_id
			ORDER BY avg_popularity DESC
		) AS source_rank FROM 
		(
			SELECT pop_measure_type_id, performer_name, COUNT(DISTINCT recording_id) AS num_recordings, ROUND(AVG(value)) AS avg_popularity
			FROM classically.performer 
			JOIN recording_performer USING (performer_id)
			JOIN recording USING (recording_id)
			JOIN popularity_measure USING (recording_id)
			GROUP BY performer_id, pop_measure_type_id
			HAVING num_recordings > minimum_recordings
			ORDER BY pop_measure_type_id, avg_popularity DESC
		) x
		ORDER BY pop_measure_type_id, avg_popularity DESC, source_rank ASC
	) y
	WHERE source_rank <= num_performers_per_service;
END$$
DELIMITER ;

