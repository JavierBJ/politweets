'''
updater.py

Script for periodic updates on the likes of tweets retrieved by the script
streaming.py. While retweets and replies are downloaded automatically by the
aforementioned script, likes are not monitored by the Streaming API. This
script re-downloads the same tweets on specified periods, in order to retrieve
the number of likes achieved up to these periods.

Author: Javier Beltran
'''

import logging
import importlib
import configparser
import pymongo
import twitter
import time
from datetime import datetime

# Logging configuration
importlib.reload(logging)
logging.basicConfig(filename='logs/Updater.log', filemode='w',
                    format='[%(levelname)s:%(asctime)s] %(message)s', level=logging.DEBUG)
logging.info('Starting')

# Config file reader
config = configparser.ConfigParser()
config.read('config.ini')

# Python-Twitter Configuration
api = twitter.Api(access_token_key=config['TWITTER']['AccessToken'], access_token_secret=config['TWITTER']['AccessTokenSecret'], consumer_key=config['TWITTER']['ConsumerKey'], consumer_secret=config['TWITTER']['ConsumerSecret'], sleep_on_rate_limit=True, tweet_mode='extended')
logging.info('Connected to Twitter')

# Configure DB to store tweets streamed
mongo = pymongo.MongoClient()
db = mongo['politweets']

update_times = [60000, 3600000, 86400000, 604800000]    # Periods in ms (1 min, 1 hour, 1 day, 1week)
update_fields = ['update_1m', 'update_1h', 'update_1d', 'update_1w']
while True:
	# Do all updates period by period
	for uptime, field in zip(update_times, update_fields):
		time_ms = time.time()*1000
		logging.info('Performing search in Mongo for ' + field)

		# This query to the DB only retrieves tweets where the updated number of likes hasn't been retrieved yet,
		# AND they are on time to be updated = they have surpassed the period from when they were downloaded.
		# This query can be performed efficiently thanks to an index on the 'timestamp_ms' field of the MongoDB collection
		cursor = db.stream_content.find({field:{'$exists':False}, 'timestamp_ms':{'$lt':str(int(time_ms-uptime))}}, batch_size=100)
		try:
			for tw in cursor:
				tweet_id = tw['id_str']
				logging.info('Getting updated status of ' + tweet_id)
				try:
					# Downloads again the same tweet from the API
					updated = api.GetStatus(tweet_id, trim_user=True, include_entities=False)
					updated_dict = updated.AsDict()
					logging.info('Updating ' + tweet_id)
					updated_time = datetime.utcfromtimestamp(time_ms/1000).strftime('%Y-%m-%d %H:%M:%S')
					added_time = datetime.utcfromtimestamp(float(tw['timestamp_ms'])/1000).strftime('%Y-%m-%d %H:%M:%S')
					favs = 0

					# If the tweet has favorites, obtain them. Otherwise, obtain 0
					if 'favorite_count' in updated_dict:
						favs = updated_dict['favorite_count']

					# Update the tweet originally downloaded to the collection, with a new field with the number of likes up to this period
					db.stream_content.update({'id_str':tweet_id},{'$set':{field:favs}})
					logging.info('Updated ' + tweet_id + ' with field ' + field + ' and value ' + str(favs) + ' (added: ' + added_time + ') (updated: ' + updated_time + ')')
				except twitter.error.TwitterError as e:
					# Tweets can be removed from Twitter but they are kept in our database. This exception controls this case.
					# We prefer to store a value anyway into the database, so this script can forget about this tweet.
					# Because the API returns an error code 144 in this case, we set the updated number of tweet to -144 in the database
					if len(e.message)>0 and ('code' in e.message[0]) and e.message[0]['code']==144:
						db.stream_content.update({'id_str':tweet_id},{'$set':{field:-144}})
						logging.info('Tweet ' + tweet_id + ' cannot be updated as it no longer exists. Setting it to -144')

					# Any other error trying to update this tweet gives it the updated value of -999
					else:
						db.stream_content.update({'id_str':tweet_id},{'$set':{field:-999}})
						logging.warning('Could not update ' + tweet_id + ' with field ' + field + ' for unknown reason: ' + str(e.message) + '. Setting it to -999')
		# Sometimes the connection to the database is closed and the cursor fails. This exception controls this cases.
		# Tweets are not updated so they will be retrieved on future iterations of the main loop
		except pymongo.errors.CursorNotFound as ec:
			logging.error('CursorNotFound, some tweets were left without update: ' + str(ec.message))
