
import tweepy
import pandas as pd
from datetime import timedelta
from pprint import pprint
import time
import uuid
import glob
import os

api = None


def get_tweets(user_id, younger):

    all_tweets = []

    tweets = []

    retries = 0
    while True:
        tweets = api.user_timeline(user_id=user_id,
                                   count=200,
                                   exclude_replies='false',
                                   tweet_mode="extended",
                                   max_id=younger)

        retries += 1

        print(
            f"...Tentativa de raspar tweets com id menor do que {younger} - resultado: {len(tweets)} ")

        if len(tweets) > 0:
            all_tweets.extend([t._json for t in tweets])
            younger = min([i.id for i in tweets]) - 1
            retries = 0
            print(f"Count de todos os tweets raspados {len(all_tweets)}")
            continue

        if retries > 9:
            break
        else:
            time.sleep(15*retries)
    return all_tweets


if(__name__ == '__main__'):
    base_path_to_csv = os.path.join(os.getcwd() + '/exported/emtweets/*.csv')
    csv_list = glob.glob(base_path_to_csv)

    min_idx = None
    if (csv_list):
        # index_col removes the duplicates
        df_list = [pd.read_csv(csv, index_col='id') for csv in csv_list]
        df = pd.concat(df_list)
        min_idx = df.index.min() - 1

    auth = tweepy.AppAuthHandler(os.environ['TWITTER_API_KEY'],
                                 os.environ['TWITTER_API_SECRET'])
    api = tweepy.API(auth)

    user = api.get_user('elonmusk')

    all_tweets = get_tweets(user.id, min_idx)
    statuses_count = user.statuses_count

    pprint(statuses_count)
    pprint(len(all_tweets))

    df = pd.json_normalize(all_tweets)
    if(not df.empty):
        df = df[['id', 'created_at', 'full_text']]
        df.to_csv('eltweets/' + str(uuid.uuid4()) + '.csv', index=False)
