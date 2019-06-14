'''
MakeTimelines.py

Downloading the whole timelines of all politicians and storing them in a MongoDB collection.

Author: Javier Beltran.
'''

import logging
import importlib
import configparser
import twitter
import pandas as pd
import pymongo

# Configure logs: print all in file, only warnings and errors in console
importlib.reload(logging)
logging.basicConfig(filename='logs/MakeTimelines.log', filemode='w',
                    format='[%(levelname)s:%(asctime)s] %(message)s', level=logging.DEBUG)

# Configure Twitter API
config = configparser.ConfigParser()
config.read('config.ini')
api = twitter.Api(access_token_key=config['TWITTER']['AccessToken'], access_token_secret=config['TWITTER']['AccessTokenSecret'], consumer_key=config['TWITTER']['ConsumerKey'], consumer_secret=config['TWITTER']['ConsumerSecret'], sleep_on_rate_limit=True, tweet_mode='extended')

# Configure MongoDB
mongo = pymongo.MongoClient()
db = mongo['politweets']

# Read accounts list
df_polits = pd.read_csv('politicians/politicians.csv', sep=';', encoding='utf-8')
politicians = list(df_polits['screen_name'].dropna())

# Retrieve timelines
logging.info('Starting to retrieve timelines')
count_total = 0
max_users = 9999999
start_idx = 0
for i,name in enumerate(politicians):
    # Change start_idx if you want to start the retrieval at some point of the list
    if i<start_idx:
        continue
    count = 0
    max_id = 0
    while True:
        # Infinite loop with exit using break statements, to allow retries after errors
        try:
            # Download a set of until 200 tweets from the API. Pagination must be handled manually,
            # setting the max_id parameter after each page of tweets is retrieved
            name_std = name.lower()
            tweets = api.GetUserTimeline(screen_name=name_std, max_id=max_id, count=200)
            tweets = [status.AsDict() for status in tweets]

            # Leave the infinite loop when no more tweets are available from this user
            if len(tweets)==0:
                logging.info(str(i) + '. Account: ' + name + ' - Additions: ' + str(count))
                break
            # While there are tweets available, insert them into the database and update the max_id
            try:
                db.timelines.insert_many(tweets)
                max_id = str(int(tweets[-1]['id_str'])-1)
                count += len(tweets)
                count_total += len(tweets)
            except:
                logging.error('Error inserting tweets to MongoDB. Closing')
                break
        except twitter.error.TwitterError as err:
            logging.info('Unavailable Account: ' + name)
            break
    # Change max_users if you want to end the retrieval at some point of the list
    if i+1>=max_users:
        logging.info('Reached max users specified: ' + str(max_users))
        break
logging.info('Finished. Timelines retrieved: ' + str(count_total))
