import pandas as pd
import datetime
import time
from datetime import datetime
from dateutil import parser

from utils.constants import Cte
def tweet_type(tweet):
    if 'retweeted_status' not in tweet:
        return Cte.TWEET

    elif isinstance(tweet['retweeted_status'], dict) and len(tweet['retweeted_status']) > 0:
        return Cte.RETWEET
    else:
        return Cte.TWEET


def tweet_creator(tweet):
    output = {}
    if 'retweeted_status' not in tweet:
        output['id_str'] = tweet['user']['id_str']
        output['id'] = int(tweet['user']['id'])
        output['screen_name'] = tweet['user']['screen_name']
    elif isinstance(tweet['retweeted_status'], dict) and len(tweet['retweeted_status']) > 0:
        output['id_str'] = tweet['retweeted_status']['user']['id_str']
        output['id'] =int(tweet['retweeted_status']['user']['id'])
        output['screen_name'] = tweet['retweeted_status']['user']['screen_name']
    else:
        output['id_str'] = tweet['user']['id_str']
        output['id'] = int(tweet['user']['id'])
        output['screen_name'] = tweet['user']['screen_name']
    return output


def create_df_from_tweet_list(tweet_list):
    if len(tweet_list) == 0:

        return pd.DataFrame(columns=['_id', 'contributors', 'coordinates', 'created_at', 'entities',
       'favorite_count', 'favorited', 'geo', 'id', 'in_reply_to_screen_name',
       'in_reply_to_status_id', 'in_reply_to_status_id_str',
       'in_reply_to_user_id', 'in_reply_to_user_id_str', 'is_quote_status',
       'lang', 'place', 'possibly_sensitive', 'quoted_status',
       'quoted_status_id', 'quoted_status_id_str', 'retweet_count',
       'retweeted', 'source', 'text', 'truncated', 'user', 'retweeted_status',
       'extended_entities', 'datetime', 'type', 'tweet_user_id',
       'tweet_user_id_str', 'tweet_screen_name', 'timestamp'])

    df = pd.DataFrame.from_dict(tweet_list)
    df.set_index(keys='id_str', inplace=True)
    df['datetime'] = df.apply(lambda x:
                              datetime.fromtimestamp(
                                  parser.parse(x['created_at']).timestamp()), axis=1)

    df['type'] = df.apply(lambda x: tweet_type(x), axis=1)
    df['tweet_user_id'] = df.apply(lambda x: tweet_creator(x)['id'], axis=1)
    df['tweet_user_id_str'] = df.apply(lambda x: tweet_creator(x)['id_str'], axis=1)
    df['tweet_screen_name'] = df.apply(lambda x:tweet_creator(x)['screen_name'], axis=1)

    df['timestamp'] = df.apply(lambda x:
                               parser.parse(x['created_at']).timestamp(), axis=1)
    # df.sort_values(by='created_at', inplace=True)

    return df
