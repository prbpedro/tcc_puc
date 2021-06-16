
import tweepy
import pandas as pd
from datetime import timedelta
from pprint import pprint
import time
import uuid

api = None


def get_tweets(user_id):

    all_tweets = []

    tweets = []

    while(len(tweets) == 0):
        tweets = api.user_timeline(user_id=user_id,
                                   count=200,
                                   exclude_replies='false',
                                   tweet_mode="extended")

        print(
            f"Tentativa de raspar tweets, primeira vez - resultado: {len(tweets)} ")
    all_tweets.extend([t._json for t in tweets])
    youger = min([i.id for i in tweets]) - 1

    retries = 0
    while True:
        tweets = api.user_timeline(user_id=user_id,
                                   count=200,
                                   max_id=youger,
                                   exclude_replies='false',
                                   tweet_mode="extended")

        retries += 1

        print(
            f"...Tentativa de raspar tweets com id menor do que {youger} - resultado: {len(tweets)} ")

        if len(tweets) > 0:
            all_tweets.extend([t._json for t in tweets])
            youger = min([i.id for i in tweets]) - 1
            retries = 0
            print(f"Count de todos os tweets raspados {len(all_tweets)}")
            continue

        if retries > 9:
            break
        else:
            time.sleep(15*retries)
    return all_tweets


if(__name__ == '__main__'):
    auth = tweepy.AppAuthHandler('JxsnGH3OKGAJL3PqvFv8B7fSz',
                                 'hgp8u1ndFlj40GN2G84n1Sf5TXbqPDJ1uTANppwCkIEsxTJtbE')
    api = tweepy.API(auth)

    user = api.get_user('elonmusk')

    all_tweets = get_tweets(user.id)
    statuses_count = user.statuses_count

    pprint(statuses_count)
    pprint(len(all_tweets))

    df = pd.json_normalize(all_tweets)

    df = df[['id', 'created_at', 'full_text', 'retweet_count']]

    df.to_csv('eltweets/' + str(uuid.uuid4()) + '.csv', index=False)
