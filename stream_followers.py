'''
stream_followers.py

This scripts retrieves lists of followers of Twitter users periodically. The
users are passed in a text file, and the list of followers is stored in a
MongoDB database, as a list of Twitter IDs.

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
from datetime import datetime


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
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# Configure DB to store tweets streamed
mongo = pymongo.MongoClient()
db = mongo['politweets']

# Load politicians accounts from csv
polits_df = pd.read_csv('politicians/politicians.csv', delimiter=';', encoding='utf-8')
polits = list(polits_df['id'].dropna())
polits = [str(x) for x in polits]

# Handle each API call
def handle(cursor):
    while True:
        try:
            yield cursor.next()  # The function returns a new follower on each iteration
        except tweepy.TweepError as e:
            print(e.response.text)
            return

# Initialize queue with all politicians at timestamp 0
queue = deque()
for polit in polits:
    queue.appendleft((polit,0))

# Check queue and download when it's time
while True:
    if len(queue)>0:
        # Check if on time to download next
        polit_id, polit_time = queue[-1]
        if time.time()>polit_time:
            queue.pop()

            # Download all follower IDs from politician
            ids = []
            while True:
                try:
                    for f_id in handle(tweepy.Cursor(api.followers_ids, user_id=polit).items()):
                        ids.append(f_id)
                    break    # Successful followers download
                except:
                    print('Error downloading followers. Retrying...')

            # Make JSON-like object to insert in Mongo
            date = datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d')
            json = {'polit_id':polit_id, 'date':date, 'followers_ids':ids}
            db.followers.insert_one(json)

            # Set next update of this user in 24h
            queue.appendleft((polit,time.time()+86400))

