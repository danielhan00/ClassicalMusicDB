CREATE TABLE `composer` (
  `composer_id` int NOT NULL AUTO_INCREMENT,
  `composer_name` varchar(250) NOT NULL,
  PRIMARY KEY (`composer_id`)
);

CREATE TABLE `performer` (
  `performer_id` int NOT NULL AUTO_INCREMENT,
  `performer_name` varchar(250) NOT NULL,
  PRIMARY KEY (`performer_id`)
);

CREATE TABLE `popularity_measure` (
  `popularity_measure_id` int NOT NULL AUTO_INCREMENT,
  `pop_measure_type_id` int NOT NULL,
  `date` datetime DEFAULT CURRENT_TIMESTAMP,
  `value` int DEFAULT NULL,
  `recording_id` int NOT NULL,
  PRIMARY KEY (`popularity_measure_id`)
);

CREATE TABLE `recording` (
  `recording_id` int NOT NULL AUTO_INCREMENT,
  `song_id` int NOT NULL,
  `recorded_date` date DEFAULT NULL,
  PRIMARY KEY (`recording_id`)
);

CREATE TABLE `recording_performer` (
  `recording_id` int NOT NULL,
  `performer_id` int NOT NULL,
  PRIMARY KEY (`recording_id`,`performer_id`)
);

CREATE TABLE `recording_source` (
  `recording_id` int NOT NULL,
  `source_id` int NOT NULL,
  `source_song_id` varchar(50) NOT NULL
);

CREATE TABLE `song` (
  `song_id` int NOT NULL AUTO_INCREMENT,
  `composer_id` int DEFAULT NULL,
  `title` varchar(250) DEFAULT NULL,
  `year` int DEFAULT NULL,
  PRIMARY KEY (`song_id`)
);

CREATE TABLE `source` (
  `source_id` int NOT NULL,
  `name` varchar(45) NOT NULL,
  PRIMARY KEY (`source_id`)
);

INSERT INTO `source` (`source_id`, `name`) 
VALUES ('1', 'Deezer'), ('2', 'Spotify'), ('3', 'last.fm'), ('4', 'tidal');

CREATE TABLE `pop_measure_type` (
  `pop_measure_type_id` int NOT NULL,
  `type_name` varchar(45) NOT NULL,
  `source_id` int DEFAULT NULL,
  PRIMARY KEY (`pop_measure_type_id`),
  UNIQUE KEY `type_name_UNIQUE` (`type_name`)
);

INSERT INTO `pop_measure_type` ( `pop_measure_type_id`, `type_name`, `source_id`)
VALUES ('1', 'deezer_rank', '1'), ('2', 'spotify_popularity', '2'), ('3', 'lastfm_playcount', '3'), ('4', 'lastfm_listeners', '3'), ('5', 'tidal_popularity', '4');

