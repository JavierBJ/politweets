import pymongo
import pandas as pd

# Configure MongoDB
mongo = pymongo.MongoClient()
db = mongo['politweets']

# Get timelines from DB with the following parameters
coll = 'timelines'
limit = 0
json = db[coll].find().limit(lim)

# DB to dataframe
tweets = []
variables = ['id_str', 'created_at', 'full_text', 'retweet_count', 'favorite_count']
for tw in json:
    tw_dict = {k:tw[k] for k in tw if k in variables}
    tw_dict['user_id'] = tw['user']['id_str']
    tw_dict['screen_name'] = tw['user']['screen_name']
    tweets.append(tw_dict)
tweets_df = pd.DataFrame(tweets)

# Normalize some values
tweets_df['favorite_count'].fillna(0, inplace=True)
tweets_df['retweet_count'].fillna(0, inplace=True)
tweets_df['full_text'] = tweets_df['full_text'].str.replace('\n',' ').str.replace('\r',' ')
tweets_df.rename(columns={'full_text':'text'}, inplace=True)

# Save as CSV
tweets_df.to_csv('tweets/timelines.csv', sep=';', encoding='utf-8', index=False)
