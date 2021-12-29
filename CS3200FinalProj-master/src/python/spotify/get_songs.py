import sys
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials # authorizes without user authentication
import pymysql.cursors
from pprint import pprint
import copy

"""
Song scraper for spotify.
Uses Spotipy: https://spotipy.readthedocs.io
And PyMySql: https://pypi.org/project/PyMySQL/

Other important sources:
https://github.com/plamere/spotipy/blob/master/examples
https://developer.spotify.com/documentation/web-api/reference


"""

"""
Specific list of playlists to scrape from. Names are here too for clarity.
"""
PLAYLISTS = {
    'spotify:playlist:37i9dQZF1DWWEJlAGA9gs0': 'Classical Essentials',
    'spotify:playlist:37i9dQZF1DX5hL1aT2vhMb': 'Women of Classical',
    'spotify:playlist:37i9dQZF1DXbaZdHeCwl9C': 'Early Classical',
    'spotify:playlist:3jh9R9uXIoozPA0uccJt2C': 'Top Picks Classical',
    'spotify:playlist:37i9dQZF1DWZm0IlR3JPVY': 'Spring Classical',
    'spotify:playlist:37i9dQZF1DX0ynPp7KaiSY': 'Winter Classical',
    'spotify:playlist:37i9dQZF1DX7cBprxbt1Fn': 'Gentle Classical',
}

SOURCE_ID = 2


def get_classical_playlists(sp):
    """
    Utility method to get all of spotify's 'classical' playlists, in case I want to do that again.
    :param sp: An authenticated spotify object
    :return: nothing, just prints
    """
    # gets "all" spotify playlists (data is paginated)
    playlists = sp.user_playlists('spotify')
    # iterate through and retrieve those with "classical" in the name
    while playlists:
        for i, playlist in enumerate(playlists['items']):
            lowername = playlist['name'].lower()
            # some design decisions on filtering here - don't want 'contemporary classical'
            if 'classical' in lowername:
                print("%4d %s %s" % (i + 1 + playlists['offset'], playlist['uri'], playlist['name']))
        if playlists['next']:
            playlists = sp.next(playlists)
        else:
            playlists = None


def insert_recording_data(obj, conn):
    """
    Inserts all data for the given recording object.
    :param obj: Spotify recording object
    :param conn: database connection
    :return: None
    """
    # disambiguate composer/performer
    # heuristic: if track has one artist, it's just a performer.
    # if track has more than one artist, the first is the composer, the remainder are performers.
    # (this is the typical format)

    # "Maybe artist"
    composer = None

    # [artist]
    performers = None
    if len(obj['artists']) == 1:
        performers = obj['artists']
    elif len(obj['artists']) > 1:
        composer = obj['artists'][0]
        performers = obj['artists'][1:]
    else:
        raise ValueError(f"Track {obj['id']} has no artists, what?")

    # do this all in one commit
    # if it crashes, it crashes, that's fine, this is a script. Want the entire transaction to fail.
    with conn.cursor() as cursor:
        composer_id = None
        if composer:
            # insert into composer
            cursor.execute("INSERT INTO `composer` (composer_name) VALUES (%s)", (composer['name']))
            # grab composer id
            cursor.execute("SELECT last_insert_id() as id")
            composer_id = cursor.fetchone()['id']

        # insert into song
        # if we have a composer
        if composer_id:
            # no way to resolve year of composition
            cursor.execute("INSERT INTO `song` (composer_id, title) VALUES (%s, %s)",
                           (composer_id, obj['name']))
            # grab song id
            cursor.execute("SELECT last_insert_id() as id")
            song_id = cursor.fetchone()['id']
        else:
            cursor.execute("INSERT INTO `song` (title) VALUES (%s)", (obj['name']))
            # grab song id
            cursor.execute("SELECT last_insert_id() as id")
            song_id = cursor.fetchone()['id']

        # insert into performer
        performer_ids = []
        for performer in performers:
            cursor.execute("INSERT INTO `performer` (performer_name) VALUES (%s)", (performer['name']))
            # grab performer id
            cursor.execute("SELECT last_insert_id() as id")
            performer_ids.append(cursor.fetchone()['id'])

        # insert into recording
        cursor.execute("INSERT INTO `recording` (song_id, recorded_date) VALUES (%s, date(%s))",
                       (song_id, f"{obj['recorded_year']}-{obj['recorded_month']}-{obj['recorded_day']}"))
        # grab recording id
        cursor.execute("SELECT last_insert_id() as id")
        recording_id = cursor.fetchone()['id']

        # insert into recording_performer
        for pid in performer_ids:
            cursor.execute("INSERT INTO `recording_performer` (recording_id, performer_id) VALUES (%s, %s)",
                           (recording_id, pid))

        # insert into recording_source
        cursor.execute("INSERT INTO `recording_source` (recording_id, source_id, source_song_id) VALUES (%s, %s, %s)",
                       (recording_id, SOURCE_ID, obj['id']))

    # commit changes
    conn.commit()
    print(f"Completed inserts for track {obj['id']}")


def preprocess_artists(arr_artist):
    """
    Preprocesses artists by splitting slash-delimited artists into separate objects.
    :param arr_artist: Array of simplified artist objects retrieved from spotify API.
    :return: Array of pseudo-artist objects, with new objects created for artists that were slash-delimited.
    """

    new_arr = []
    for a in arr_artist:
        if '/' in a['name']:
            sub_names = a['name'].split('/')
            for sname in sub_names:
                new_obj = copy.deepcopy(a)
                new_obj['name'] = sname
                new_arr.append(new_obj)
        else:
            new_arr.append(a)

    # dedupe
    name_array = []
    new_new_arr = []
    for obj in new_arr:
        if obj['name'] not in name_array:
            name_array.append(obj['name'])
            new_new_arr.append(obj)
    return new_new_arr

def get_songs(schema):
    """
    Gets classical songs from spotify.
    :param schema: Classic.ly schema to insert data into.
    :return: Nothing.
    Side effects: INSERTs into recording, song, performer, composer, source, recording_source, recording_performer.
    """

    # authenticate with spotify
    # requires that SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET environment variables are set
    auth_manager = SpotifyClientCredentials()
    sp = spotipy.Spotify(auth_manager=auth_manager)

    print("Retrieving recordings from:")
    pprint(PLAYLISTS)

    # iterate through playlists and scrape recordings
    scrape_recordings = set()
    for pl in PLAYLISTS.keys():
        offset = 0
        # get items, only want to store a set of IDs for now
        pl_items = sp.playlist_items(pl, offset=offset, fields="items.track.id", additional_types=("track",))
        while True:
            for it in pl_items['items']:
                if it['track']:
                    scrape_recordings.add(it['track']['id'])
            if len(pl_items['items']) == 0:
                break
            # increment offset and re-request
            # it's garbage that I have to do this manually
            offset = offset + len(pl_items['items'])
            pl_items = sp.playlist_items(pl, offset=offset, fields="items.track.id", additional_types=("track",))

    print(f"Retrieved {len(scrape_recordings)} recordings.")

    # iterate through obtained recordings and look for composer, performer, song
    recording_objects = []
    for rec_id in scrape_recordings:
        track = sp.track(rec_id)
        recording_objects.append({
            'id': track['id'],
            'name': track['name'],
            'artists': preprocess_artists(track['artists']),
            'popularity': track['popularity'],
            'recorded_year': track['album']['release_date'][:4],
            'recorded_month': track['album']['release_date'][5:7] if track['album']['release_date_precision'] != 'year'
                else '00',
            'recorded_day': track['album']['release_date'][8:] if track['album']['release_date_precision'] == 'day'
                else '00'
        })

    # TIME TO DO SQL

    # establish SQL connection
    # Connect to the database
    connection = pymysql.connect(host='classically.c986fzuamnpo.us-east-1.rds.amazonaws.com',
                                 user='admin',
                                 password='INSERTPASSHERE',
                                 db=schema,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:  # if this fails, its most likely because we've run this script before, that's fine
        with connection.cursor() as cursor:
            # Add spotify source
            sql = "INSERT INTO `source` (`source_id`, `name`) VALUES (%s, %s)"
            cursor.execute(sql, (SOURCE_ID, 'spotify'))

        # connection is not autocommit by default.
        connection.commit()

    finally:

        # add a record for each recording we've retrieved
        try:
            for obj in recording_objects:
                insert_recording_data(obj, connection)

        finally:
            # close connection
            connection.close()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python3 get_songs.py <DB>")
    get_songs(sys.argv[1])
