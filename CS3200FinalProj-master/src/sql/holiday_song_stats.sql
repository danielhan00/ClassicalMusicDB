use classically;  


drop procedure if exists get_holiday_song_stats;

DELIMITER //

CREATE PROCEDURE get_holiday_song_stats
(  
	holiday_song_title VARCHAR(255)
)
BEGIN
	SELECT popularity_measure.pop_measure_type_id, title, popularity_measure.value AS 'popularity_value', source.name AS 'source', popularity_measure.date
	FROM classically.popularity_measure
		JOIN recording ON recording.recording_id = popularity_measure.recording_id
		JOIN song ON song.song_id = recording.song_id
		JOIN pop_measure_type ON pop_measure_type.pop_measure_type_id = popularity_measure.pop_measure_type_id
		JOIN source ON source.source_id = pop_measure_type.source_id
	WHERE title = holiday_song_title
	ORDER BY pop_measure_type_id, popularity_measure.date;
        END //   
        
DELIMITER ;

CALL get_holiday_song_stats('Christmas Canon');
CALL get_holiday_song_stats('O Holy Night');
CALL get_holiday_song_stats('Messiah: No. 7. "And He shall purify" from Part One');
CALL get_holiday_song_stats('Dance of the Sugar Plum Fairy');
CALL get_holiday_song_stats('Moonlight Sonata');

    

