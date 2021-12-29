import sys
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials # authorizes without user authentication
import pymysql.cursors
from pprint import pprint

# METRICS:
METRICS = {
    'FOUND_COMPOSER' : 0,
    'FOUND_MORE_THAN_ONE_COMPOSER': 0,
    'NUM_SEARCHED' : 0
}


def create_song_object(song_id, title, db_conn):
    """
    Generates a "song object" using data in Classicly database.
    :param song_id: Song ID to search for
    :param title: Song title, used when constructing obj
    :param db_conn: Databse connection
    :return: Object of format {"title": title, "recordings": {recording id: {"performers": [names]} }}
    """
    new_obj = {'title': title, 'recordings': {}}
    # get all recordings for this song
    with db_conn.cursor() as cursor:
        recording_query = """
            SELECT 
                recording_id
            FROM recording
            WHERE song_id = %s
        """
        cursor.execute(recording_query, (song_id,))
        db_conn.commit()
        for recording_row in cursor.fetchall():
            recording_id = recording_row['recording_id']


            # get performers
            perfomer_query = """
                SELECT 
                    performer_name
                FROM
                    performer
                    JOIN recording_performer USING (performer_id)
                WHERE recording_id = %s
            """
            cursor.execute(perfomer_query, (recording_id,))
            rec_obj = {
                "performers": [row['performer_name'] for row in cursor.fetchall()]
            }
            new_obj['recordings'][recording_id] = rec_obj

        return new_obj


def get_composer_from_spotify(sp, song_obj):
    """
    Attempt to retrieve the given song's composer from spotify, along with the track ID.
    Also return the recording ID that found the composer, to augment recording_source later.
    :param sp: Spotify object.
    :param song_obj: Dictionary representing song.
                    Format: {"title": title, "recordings": {recording id: {"performers": [names]} }}
    :return: Tuple of: (composer_name, (recording_id, spotify_track_id))
    """

    METRICS['NUM_SEARCHED'] = METRICS['NUM_SEARCHED'] + 1
    possible_composers = set()
    ids = []
    for rec_id in song_obj['recordings'].keys():
        rec_obj = song_obj['recordings'][rec_id]
        # look for the song
        query = f'track:{song_obj["title"]}'
        if len(rec_obj['performers']) > 0:
            query = query + f' artist:{" ".join(rec_obj["performers"])}'

        result = sp.search(query, type='track')
        res_tracks = result['tracks']['items']
        if len(res_tracks) > 0:
            # for now take first track
            track_id = res_tracks[0]['id']
            # get track from spotify
            track_obj = sp.track(track_id)
            track_artist_names = [a['name'] for a in track_obj['artists']]
            # heuristic from get_songs: first artist is composer
            possible_composers.add(track_artist_names[0])
            ids.append((rec_id, track_id))
    # print(f"TITLE: {song_obj['title']}")
    # pprint(possible_composers)
    # throw out cases where we have multiple composers; we have no way to know which is correct
    if len(possible_composers) == 1:
        METRICS['FOUND_COMPOSER'] = METRICS['FOUND_COMPOSER'] + 1
        return possible_composers.pop(), ids[0]
    elif len(possible_composers) > 1:
        METRICS['FOUND_MORE_THAN_ONE_COMPOSER'] = METRICS['FOUND_MORE_THAN_ONE_COMPOSER'] + 1
        return None


def insert_composer(db_conn, song_id, composer_name, recording_id, spotify_track_id):
    """
    Performs a series of database inserts/updates with newly retrieved composer.
    :param db_conn: Database connection
    :param song_id: Classicly song ID
    :param composer_name: Name of recovered composer to insert
    :param spotify_track_id: Track ID from spotify, to add to recording_source
    :param recording_id: Classicly recording ID corresponding to recording that was used to recover composer
    :return: None
    """
    with db_conn.cursor() as cursor:
        comp_ins_query = """
        INSERT INTO `composer` (composer_name)
        VALUES (%s)
        """
        cursor.execute(comp_ins_query, (composer_name,))
        # grab composer id
        cursor.execute("SELECT last_insert_id() as id")
        composer_id = cursor.fetchone()['id']

        song_update_query = """
        UPDATE `song` SET composer_id = %s
        WHERE song_id = %s
        """
        cursor.execute(song_update_query, (composer_id, song_id))

        recording_source_ins = """
        INSERT INTO `recording_source` (recording_id, source_id, source_song_id)
        VALUES (%s, %s, %s)
        """
        cursor.execute(recording_source_ins, (recording_id, 2, spotify_track_id))
    db_conn.commit()
    print(f"Completed update for song ID {song_id}")


def composer_backfill(db):
    """
    Attempts to backfill composer_id field of songs that lack it by searching for them on spotify.
    :param db: Database to run against
    :return: None. Prints metrics at the end.
    """
    # for every song with null composer_id
    # {song id: {"title": title, "recordings": {recording id: {"performers": [names]} }}}
    song_dict = {}


    # sql authentication
    # establish SQL connection
    # Connect to the database
    db_conn = pymysql.connect(host='classically.c986fzuamnpo.us-east-1.rds.amazonaws.com',
                                 user='admin',
                                 password='INSERTPASSHERE',
                                 db=db,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    # get all songs that lack a composer, plus their recordings/performers
    try:
        with db_conn.cursor() as cursor:
            query = """
                SELECT 
                    song_id,
                    title
                FROM song
                WHERE composer_id is NULL
            """
            cursor.execute(query)
            for obj in cursor.fetchall():
                song_dict[obj['song_id']] = create_song_object(obj['song_id'], obj['title'], db_conn)
    finally:
        db_conn.close()

    # open spotify connection
    # authenticate with spotify
    # requires that SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET environment variables are set
    auth_manager = SpotifyClientCredentials()
    sp = spotipy.Spotify(auth_manager=auth_manager)

    #print("SONG DICT")
    #pprint(song_dict)
    for song_id in song_dict.keys():
        # check spotify and look for a composer
        composer_ids_tuple = get_composer_from_spotify(sp, song_dict[song_id])
        if composer_ids_tuple:
            # insert a new composer and update song row, recording_source
            insert_composer(db_conn, song_id, composer_ids_tuple[0], composer_ids_tuple[1][0], composer_ids_tuple[1][1])

    pprint(METRICS)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python3 composer_backfill.py <DB>")
    composer_backfill(sys.argv[1])