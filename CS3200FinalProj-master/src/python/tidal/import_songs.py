import tidalapi
from datetime import datetime
import pymysql.cursors
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('user', type=str, help="tidal username")
parser.add_argument('password', type=str, help="tidal password")
parser.add_argument('db_password', type=str, help="Database password for mysql database")
args = parser.parse_args()

USER = args.user
PASS = args.password
DB_PASS = args.db_password

playlists = {
'Home Orchestra': '32f9c4df-c05f-4b1d-b355-30e4f5c26e91',
'Classical Sleep': '0107145b-ef1f-4479-9903-a70f637104e5',
'Classical Relaxation': '44c3f760-0537-4449-a310-7400a43c2080',
'Classical Romance': '60a3900d-b28f-4cc1-9846-a556d0f0f930',
'Classical Focus': '806558be-48ef-42fd-af82-0d70fa49556e',
'Classical Kids': '1d8300a9-2803-4d6c-87a7-d4597fc6c928',
'Classical at the Movies': '23242a77-07f9-4c82-bb63-5ee3456a36f4',
'Classical Piano': 'b870775d-4f0d-4f76-b21b-11a670d81a70',
'Baroque and Beyond': '5a4e836e-5623-4424-b25d-992a80702c9c',
'Classical Ballet': 'f640027c-2c38-4bab-b3b6-ab4a7a3c4669',
'Opera Moderna': '419b8192-00fb-4892-a475-84da8609f776',
'Piano Spheres': 'b54f6cf2-96f7-4eaf-8c4c-15ad38d365f9',
'Classical Classics': 'de10425c-026c-44e2-add5-68b3714637e1'
}

SOURCE_ID = 4 # Putting tidal as source 4
schema = 'test'

#source
name = "Tidal"

def insert_track(track, conn):
    #composer
    #composer_name = ?

    #song
    title = track.name
    #year = ?

    #recording
    recorded_date = track.album.release_date
    recorded_date_str = recorded_date.strftime("%y-%m-%d")

    #performer
    artists = track.artists # artists are performers
    performers = [a.name for a in artists]

    #popularity_measure
    date = datetime.now()
    rank = track.popularity

    #recording_source
    recording_source_id = track.id

    with conn.cursor() as cursor:
        #missing composer

        # song insert
        cursor.execute("INSERT INTO `song` (title) VALUES (%s)", (title))

        cursor.execute("SELECT last_insert_id() as id")
        song_id = cursor.fetchone()['id']

        # performer insert
        performer_ids = []
        for performer in performers:
            cursor.execute("INSERT INTO `performer` (performer_name) VALUES (%s)", (performer))
            cursor.execute("SELECT last_insert_id() as id")
            performer_ids.append(cursor.fetchone()['id'])

        # recording insert
        cursor.execute("INSERT INTO `recording` (song_id, recorded_date) VALUES (%s, %s)", (song_id, recorded_date_str))
        cursor.execute("SELECT last_insert_id() as id")
        recording_id = cursor.fetchone()['id']

        # recording_performer insert
        for pid in performer_ids:
            cursor.execute("INSERT INTO `recording_performer` (recording_id, performer_id) VALUES (%s, %s)", (recording_id, pid))

        # recording_source insert
        cursor.execute("INSERT INTO `recording_source` (recording_id, source_id, source_song_id) VALUES (%s, %s, %s)", (recording_id, SOURCE_ID, recording_source_id))

    # commit changes
    conn.commit()
    print("Completed inserts for track ", title)


def process_playlists():
    # establish SQL connection
    # Connect to the database
    connection = pymysql.connect(host='classically.c986fzuamnpo.us-east-1.rds.amazonaws.com',
                                 user='admin',
                                 password= DB_PASS,
                                 db=schema,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            # Add tidal source
            sql = "INSERT INTO `source` (`source_id`, `name`) VALUES (%s, %s)"
            cursor.execute(sql, (SOURCE_ID, 'tidal'))

        # connection is not autocommit by default.
        connection.commit()

    finally:
        try:
            session = tidalapi.Session()
            session.login(USER, PASS)
            for playlist_id in playlists.values():
                tracks = session.get_playlist_tracks(playlist_id)
                for track in tracks:
                    insert_track(track, connection)

        finally:
            # close connection
            connection.close()

if __name__ == '__main__':
    process_playlists()

# Track example
#{'available': True, 'album': <tidalapi.models.Album object at 0x109777780>, 'disc_num': 2, 'version': None, 'type': None, 'duration': 91, 'artist': <tidalapi.models.Artist object at 0x102d88710>, 'id': 12623855, 'artists': [<tidalapi.models.Artist object at 0x10a01d4e0>], 'track_num': 11, 'popularity': 14, 'name': '29 Canoni: No. 11. Andantino tranquillo, con molto sentimento'}

# Album example
#{'duration': None, 'num_discs': None, 'artist': <tidalapi.models.Artist object at 0x10912b898>, 'release_date': datetime.datetime(1978, 8, 1, 0, 0), 'num_tracks': None, 'id': 23623055, 'name': "David Bowie narrates Prokofiev's Peter and the Wolf & The Young Person's Guide to the Orchestra", 'artists': [<tidalapi.models.Artist object at 0x10912b748>]}
