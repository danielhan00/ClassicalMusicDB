import requests
import json
import time
from IPython.core.display import clear_output
import pandas as pd
import pymysql
import itertools

API_KEY = 'ae04648ed88d951f66e6a5831e119fcc'
USER_AGENT = 'nkhatana'
SOURCE_ID = 3


# logs in and gets the responses from lastfm with the payload
# payload: JSON request
def lastfm_get(payload):
	# define headers and URL
	headers = {'user-agent': USER_AGENT}
	url = 'http://ws.audioscrobbler.com/2.0/'

	# Add API key and format to the payload
	payload['api_key'] = API_KEY
	payload['format'] = 'json'

	response = requests.get(url, headers=headers, params=payload)
	return response

# gets the top tracks tagged classical
# returns array of responses dictionaries 
def get_classical_responses():
	responses = []

	page = 1
	has_more_songs = True

	while has_more_songs:
		payload = {
			'method': 'tag.gettoptracks',
			'tag': 'classical',
			'limit': 500,
			'page': page
		}

		# print some output so we can see the status
		print("Requesting page {}".format(page))
		# clear the output to make things neater
		clear_output(wait = True)

		# make the API call
		response = lastfm_get(payload)

		# if we get an error, print the response and halt the loop
		if response.status_code != 200:
			print(response.text)
			break

		# extract pagination info
		page = int(response.json()['tracks']['@attr']['page'])
		total_pages = int(response.json()['tracks']['@attr']['totalPages'])

		if len(response.json()['tracks']['track']) > 0:
			responses.append(response)
		else:
			has_more_songs = False

		# if it's not a cached result, sleep
		if not getattr(response, 'from_cache', False):
			time.sleep(0.25)

		# increment the page number
		page += 1

	return responses

# inserts artists, track and track_id from lastfm api response
# takes in lastfm api responses
def insert_responses(responses):
	for each in responses:
		r0 = responses[0]
		r0_json = r0.json()
		r0_tracks = r0_json['tracks']['track']
		r0_df = pd.DataFrame(r0_tracks)
		tracks = r0_df['name']
		artists = r0_df['artist']
		track_id = r0_df['mbid']
		tracks = tracks.to_list()
		for key, value in artists.items():
			artists[key] = artists[key].get('name')
		artists = artists.to_list()
		track_ids = track_id.to_list()
		insert_response_into_db(artists, tracks, track_ids, SOURCE_ID)
		print("inserted ", len(each.json()['tracks']['track']), " tracks")


def insert_lastfm_source():
	# Insert into source
	# Connect to the database
    connection = pymysql.connect(host='classically.c986fzuamnpo.us-east-1.rds.amazonaws.com',
                             user='admin',
                             password='password',
                             db='test')

    cursor=connection.cursor()


    # Insert into source
    sql = "INSERT INTO `source` (`source_id`, `name`) VALUES (%s, %s)"
    cursor.execute(sql, (SOURCE_ID, 'last.fm'))

    # the connection is not autocommitted by default, so we must commit to save our changes
    connection.commit()


def insert_response_into_db(artists, tracks, track_ids, source_id):
	try:
	# Connect to the database
		connection = pymysql.connect(host='classically.c986fzuamnpo.us-east-1.rds.amazonaws.com',
								 user='admin',
								 password='password',
								 db='test')

		cursor=connection.cursor()



		# parallel loop through artists and tracks
		for (performer,song,track_id) in zip(artists, tracks, track_ids):

			if(track_id != ""):
			
				# insert performer name into performer table
				sql_performer = "INSERT INTO `performer` (`performer_name`) VALUES (%s)"
				cursor.execute(sql_performer, performer)
				
				# grab performer_id
				performer_id = cursor.lastrowid
				
				# insert song title into song table
				sql_song = "INSERT INTO `song` (`title`) VALUES (%s)"
				cursor.execute(sql_song, song)
				
				# grab song_id
				song_id = cursor.lastrowid
				
				# insert song_id into recording table
				sql_recording = "INSERT INTO `recording` (`song_id`) VALUES (%s)"
				cursor.execute(sql_recording, song_id)
				
				# grab recording_id
				recording_id = cursor.lastrowid
				
				# insert recording_id and corresponding performer_id into recording_performer table
				sql_rp = "INSERT INTO `recording_performer` (`recording_id`, `performer_id`) VALUES (%s, %s)"
				cursor.execute(sql_rp, (recording_id, performer_id))
		

				sql_rp = "INSERT INTO `recording_source` (`recording_id`, `source_id`, `source_song_id` ) VALUES (%s, %s, %s)"
				cursor.execute(sql_rp, (recording_id, SOURCE_ID, track_id))
			

		# the connection is not autocommitted by default, so we must commit to save our changes
		connection.commit()

	finally:
		connection.close()





if __name__ == '__main__':
	responses = get_classical_responses()
	print(len(responses))
	insert_responses(responses)
	insert_lastfm_source()

