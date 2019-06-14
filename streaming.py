'''
streaming.py

Script for downloading tweets in real time using the Twitter Streaming API.
It streams a set of user accounts specified in a text file and, every time
a tweet is retrieved, it is stored in a MongoDB database. Depending on the
nature of the tweet, it is stored on a certain collection of the database.

Author: Javier Beltran
'''

import logging
import importlib
import tweepy
import twitter
import configparser
import pandas as pd
import pymongo
from collections import deque
import datetime
import time
import json

# Logging configuration
importlib.reload(logging)
logging.basicConfig(filename='logs/Streaming.log', filemode='w',
                    format='[%(levelname)s:%(asctime)s] %(message)s', level=logging.DEBUG)

# Config file reader
config = configparser.ConfigParser()
config.read('config.ini')

# Tweepy Configuration
auth = tweepy.OAuthHandler(config['TWITTER']['ConsumerKey'], config['TWITTER']['ConsumerSecret'])
auth.set_access_token(config['TWITTER']['AccessToken'], config['TWITTER']['AccessTokenSecret'])

# Python-Twitter Configuration
api = twitter.Api(access_token_key=config['TWITTER']['AccessToken'], access_token_secret=config['TWITTER']['AccessTokenSecret'], consumer_key=config['TWITTER']['ConsumerKey'], consumer_secret=config['TWITTER']['ConsumerSecret'], sleep_on_rate_limit=True, tweet_mode='extended')

# Configure DB to store tweets streamed
mongo = pymongo.MongoClient()
db = mongo['politweets']

# Load politicians accounts from csv
polits_df = pd.read_csv('politicians/politicians.csv', delimiter=';', encoding='utf-8')
polits = list(polits_df['id'].dropna())
polits = [str(x) for x in polits]
polits_set = set(polits)

# Stream Listener to receive tweets online
class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        json = status._json

	# Check if goes to stream_content or stream_extra
        user = json['user']['id_str']
        if (user in polits_set) and ('retweeted_status' not in json):
            db.stream_content.insert_one(json)
            logging.info('Inserted tweet as content: ' + status.id_str)
        else:
            db.stream_extra.insert_one(json)
            logging.info('Inserted tweet as extra: ' + status.id_str)
        t = time.time()

    def on_error(self, status_code):
        logging.warning('Error code: ' + status_code)

# Create Streaming
# The streaming library creates a listener on a different thread. Its execution is determined
# by the class passed: MyStreamListener()
stream = tweepy.Stream(auth, MyStreamListener(), tweet_mode='extended')
while True:
    try:
        stream.filter(follow=polits)
    except Exception as e:
        logging.warning('Connection failed with exception ' + type(e).__name__ + '. Relaunching')
