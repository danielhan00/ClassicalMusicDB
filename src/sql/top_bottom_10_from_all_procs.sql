-- Stored procedures
use test;
use classically;

drop procedure if exists top_10_from_all;
DELIMITER //
CREATE PROCEDURE top_10_from_all()
BEGIN
	DECLARE latest_date date;
    
    select max(date(date)) into latest_date from popularity_measure;
    
	WITH flattened_composers AS (
    SELECT
        recording_id,
        GROUP_CONCAT(performer_name SEPARATOR ', ') as performers
    FROM recording_performer
    JOIN performer USING (performer_id)
    GROUP BY recording_id
	),
	all_recs_ranked AS (
		SELECT
			title,
			composer_name,
			performers,
			recorded_date,
			DATE(date) as pop_date,
			name as service_name,
			row_number() OVER(PARTITION BY pop_measure_type_id, DATE(date) ORDER BY value DESC) AS service_rank_noties,
			type_name AS measure_type,
            value
		FROM song
		JOIN composer USING (composer_id)
		JOIN recording USING (song_id)
		JOIN flattened_composers USING (recording_id)
		JOIN popularity_measure USING (recording_id)
		JOIN pop_measure_type USING (pop_measure_type_id)
		JOIN source USING (source_id)
	)
	SELECT * from all_recs_ranked
	WHERE service_rank_noties <= 10
		AND pop_date = latest_date
	ORDER BY service_name, service_rank_noties ASC;
END //
DELIMITER ;

call top_10_from_all();

drop procedure if exists bottom_10_from_all;
DELIMITER //
CREATE PROCEDURE bottom_10_from_all()
BEGIN
	DECLARE latest_date date;
    
    select max(date(date)) into latest_date from popularity_measure;
    
	WITH flattened_composers AS (
    SELECT
        recording_id,
        GROUP_CONCAT(performer_name SEPARATOR ', ') as performers
    FROM recording_performer
    JOIN performer USING (performer_id)
    GROUP BY recording_id
	),
	all_recs_ranked AS (
		SELECT
			title,
			composer_name,
			performers,
			recorded_date,
			DATE(date) as pop_date,
			name as service_name,
			row_number() OVER(PARTITION BY pop_measure_type_id, DATE(date) ORDER BY value ASC) AS service_rank_noties,
			type_name AS measure_type,
            value
		FROM song
		JOIN composer USING (composer_id)
		JOIN recording USING (song_id)
		JOIN flattened_composers USING (recording_id)
		JOIN popularity_measure USING (recording_id)
		JOIN pop_measure_type USING (pop_measure_type_id)
		JOIN source USING (source_id)
	)
	SELECT * from all_recs_ranked
	WHERE service_rank_noties <= 10
		AND pop_date = latest_date
	ORDER BY service_name, service_rank_noties ASC;
END //
DELIMITER ;

call bottom_10_from_all();
