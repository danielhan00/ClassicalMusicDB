use classically;

drop procedure if exists composer_popularity_change_on_service;

-- Key for pop_measure_type:
-- 1: deezer rank
-- 2: spotify popularity
-- 3: lastfm playcount
-- 4: lastfm listeners
-- 5: tidal popularity


-- Procedure gets the average change in popularity for a composer's recordings on a specific source
-- Shows the 10 highest changes in popularity
DELIMITER //
CREATE PROCEDURE composer_popularity_change_on_service(in pop_measure_type int)
BEGIN
SELECT
	pop_measure_type,
    composer_id,
    composer_name,
    min_date,
    max_date,
    AVG(pct_change) AS avg_change,
    COUNT(composer_id) AS num_recordings
FROM
    (SELECT 
        *,
            ((ending_value - starting_value) / ending_value) * 100 AS pct_change
    FROM
        (SELECT 
        m.pop_measure_type_id,
            m.recording_id,
            (SELECT 
                    value
                FROM
                    popularity_measure
                WHERE
                    recording_id = s.recording_id
                        AND date = s.min_date
                        AND pop_measure_type_id = s.pop_measure_type_id) AS starting_value,
            value AS ending_value,
            min_date,
            max_date
    FROM
        popularity_measure m
    JOIN (SELECT 
        recording_id,
            MIN(date) AS min_date,
            MAX(date) AS max_date,
            pop_measure_type_id
    FROM
        popularity_measure
    JOIN recording_source USING (recording_id)
    WHERE
        pop_measure_type_id = pop_measure_type
    GROUP BY recording_id , pop_measure_type_id , source_id
    HAVING COUNT(DISTINCT source_song_id) = 1) s ON m.recording_id = s.recording_id
        AND m.date = s.max_date
        AND m.pop_measure_type_id = s.pop_measure_type_id) x
    JOIN recording USING (recording_id)
    JOIN song USING (song_id)
    JOIN composer USING (composer_id)) pct_changes
GROUP BY composer_id
ORDER BY avg_change DESC;

END //
DELIMITER ;

call composer_popularity_change_on_service(1);
call composer_popularity_change_on_service(2);
call composer_popularity_change_on_service(3);
call composer_popularity_change_on_service(4);
call composer_popularity_change_on_service(5);


drop procedure if exists performer_popularity_change_on_service;

-- Procedure gets the average change in popularity for a performers's recordings on a specific source
-- Shows the 10 highest changes in popularity
DELIMITER //
CREATE PROCEDURE performer_popularity_change_on_service(in pop_measure_type int)
BEGIN
SELECT
	pop_measure_type,
    performer_id,
    performer_name,
    min_date,
    max_date,
    AVG(pct_change) AS avg_change,
    COUNT(performer_id) AS num_recordings
FROM
    (SELECT 
        *,
            ((ending_value - starting_value) / ending_value) * 100 AS pct_change
    FROM
        (SELECT 
        m.pop_measure_type_id,
            m.recording_id,
            (SELECT 
                    value
                FROM
                    popularity_measure
                WHERE
                    recording_id = s.recording_id
                        AND date = s.min_date
                        AND pop_measure_type_id = s.pop_measure_type_id) AS starting_value,
            value AS ending_value,
            min_date,
            max_date
    FROM
        popularity_measure m
    JOIN (SELECT 
        recording_id,
            MIN(date) AS min_date,
            MAX(date) AS max_date,
            pop_measure_type_id
    FROM
        popularity_measure
    JOIN recording_source USING (recording_id)
    WHERE
        pop_measure_type_id = pop_measure_type
    GROUP BY recording_id , pop_measure_type_id , source_id
    HAVING COUNT(DISTINCT source_song_id) = 1) s ON m.recording_id = s.recording_id
        AND m.date = s.max_date
        AND m.pop_measure_type_id = s.pop_measure_type_id) x
    JOIN recording USING (recording_id)
    JOIN song USING (song_id)
    JOIN recording_performer USING (recording_id)
    JOIN performer USING (performer_id)) pct_changes
GROUP BY performer_id
ORDER BY avg_change DESC;

END //
DELIMITER ;

call performer_popularity_change_on_service(1);
call performer_popularity_change_on_service(2);
call performer_popularity_change_on_service(3);
call performer_popularity_change_on_service(4);
call performer_popularity_change_on_service(5);