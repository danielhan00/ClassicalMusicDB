import sys
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials # authorizes without user authentication
import pymysql.cursors
from pprint import pprint
import requests
import json
import os
import tidalapi
from threading import Thread
from queue import PriorityQueue
from retry import retry
from dataclasses import dataclass, field
from typing import Any
import random

"""
Retrieves available popularity measures on the day it is run.
"""

# Small helper class to use for a PriorityQueue
@dataclass(order=True)
class PrioritizedItem:
    priority: int
    item: Any=field(compare=False)

# number of rows to retrieve at once
PAGE_SIZE = 100

# ground truth for popularity measure type PKs
POP_MEASURE_TYPE_KEYS = {
    "deezer_rank": 1,
    "spotify_popularity": 2,
    "lastfm_playcount": 3,
    "lastfm_listeners": 4,
    "tidal_popularity": 5,
}


def pop_measure_ins(db_conn, pop_measure_type_id, rec_id, value):
    """
    Inserts a value into popularity_measure with the specified type.
    """
    with db_conn.cursor() as cursor:
        ins_stmt = """
        INSERT INTO `popularity_measure`
            (pop_measure_type_id, value, recording_id)
        VALUES (%s, %s, %s)
        """
        cursor.execute(ins_stmt, (pop_measure_type_id, value, rec_id))
    db_conn.commit()

@retry(tries=5,backoff=2,jitter=(1,10),delay=1,max_delay=16)
def record_deezer(db_conn, rec_id, deez_song_id):
    # retrieve song record from deezer
    result = requests.get(f"https://api.deezer.com/track/{deez_song_id}")
    result_dict = json.loads(result.text)
    pop_number = result_dict['rank']
    # insert
    pop_measure_ins(db_conn, POP_MEASURE_TYPE_KEYS['deezer_rank'], rec_id, pop_number)
    pprint(f"Inserted deezer rank {pop_number} for recording {rec_id}")


SPOTIFY = None

def record_spotify(db_conn, rec_id, spotify_track_id):
    global SPOTIFY
    if SPOTIFY is None:
        # authenticate with spotify
        # requires that SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET environment variables are set
        auth_manager = SpotifyClientCredentials()
        SPOTIFY = spotipy.Spotify(auth_manager=auth_manager)

    track_obj = SPOTIFY.track(spotify_track_id)
    pop = track_obj['popularity']
    pop_measure_ins(db_conn, POP_MEASURE_TYPE_KEYS['spotify_popularity'], rec_id, pop)
    pprint(f"Inserted spotify popularity {pop} for recording {rec_id}")

@retry(tries=5,backoff=2,jitter=(1,5),delay=2,max_delay=15)
def record_lastfm(db_conn, rec_id, musicbrainz_id):
    api_key = os.getenv("LASTFM_API_KEY")
    r = requests.get(f"http://ws.audioscrobbler.com/2.0/?method=track.getInfo&api_key={api_key}&mbid={musicbrainz_id}&format=json")
    r_json = json.loads(r.text)
    playcount = r_json['track']['playcount']
    listeners = r_json['track']['listeners']

    pop_measure_ins(db_conn, POP_MEASURE_TYPE_KEYS['lastfm_playcount'], rec_id, playcount)
    pprint(f"Inserted lastfm playcount {playcount} for recording {rec_id}")
    pop_measure_ins(db_conn, POP_MEASURE_TYPE_KEYS['lastfm_listeners'], rec_id, listeners)
    pprint(f"Inserted lastfm listeners {listeners} for recording {rec_id}")


TIDAL = None


def record_tidal(db_conn, rec_id, tidal_id):
    global TIDAL
    if TIDAL is None:
        TIDAL = tidalapi.Session()
        TIDAL.login(os.getenv("TIDAL_USER"), os.getenv("TIDAL_PASS"))

    track = TIDAL.get_track(tidal_id)
    pop = track.popularity
    pop_measure_ins(db_conn, POP_MEASURE_TYPE_KEYS['tidal_popularity'], rec_id, pop)
    pprint(f"Inserted tidal popularity {pop} for recording {rec_id}")


# recording functions
REC_FUNCS = {
    1: record_deezer,
    2: record_spotify,
    3: record_lastfm,
    4: record_tidal
}

def retrieve_insert_pop_values(db_conn, rec_id, source_id, source_song_id):
    """
    Retrieves popularity data from the specified source for the specified recording.
    :param db_conn: Classicly Database Connection
    :param rec_id: Recording ID
    :param source_id: Source ID
    :param source_song_id: Unique ID from API source
    :return: None
    """
    # This uses some assumptions about primary keys in source
    try:
        REC_FUNCS[source_id](db_conn, rec_id, source_song_id)
    except Exception as e:
        pprint(f"ERROR: Unexpected error: {e} on recording {rec_id}")


def process_rows(db, password, queue):
    conn = pymysql.connect(host='classically.c986fzuamnpo.us-east-1.rds.amazonaws.com',
                           user='admin',
                           password=password,
                           db=db,
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)

    while not queue.empty():
        row = queue.get().item
        retrieve_insert_pop_values(conn, row['recording_id'], row['source_id'], row['source_song_id'])
        queue.task_done()


def get_popularity_today(db, password):
    """
    Retrieves available popularity measures on the day it is run.
    :param db: Database to query/insert into.
    :return: None
    """
    # establish database connection
    # Connect to the database
    conn = pymysql.connect(host='classically.c986fzuamnpo.us-east-1.rds.amazonaws.com',
                                 user='admin',
                                 password=password,
                                 db=db,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    with conn.cursor() as cursor:
        select_rec_source = """
            SELECT 
                recording_id,
                source_id,
                source_song_id
            FROM `recording_source`;
        """
        cursor.execute(select_rec_source)
        page = cursor.fetchmany(PAGE_SIZE)

        q = PriorityQueue()
        while len(page) > 0:
            for row in page:
                # Random priority helps avoid long stretches from the same source and hitting rate limits
                q.put(PrioritizedItem(item=row,priority=random.randint(0,100)))
            # loop
            page = cursor.fetchmany(PAGE_SIZE)

        for i in range(0, 25):
            thread = Thread(target=process_rows, args=(db, password, q), daemon=True)
            thread.start()

        q.join()



if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python3 get_songs.py <DB> <DB password>")
    get_popularity_today(sys.argv[1], sys.argv[2])

def lambda_handler(event, context):
    get_popularity_today(os.getenv("DB"), os.getenv("DB_PASS"))
