import datetime
import pickle
from datetime import datetime

import pandas as pd
import preprocessor as tweetp
from dateutil import parser
from text_preprocessing import preprocess_text

from EasyTwitterAPI.utils.constants import Cte


def save_obj(filename, obj):
    with open(filename , 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)


def length(a):
    if a is None:
        return 0
    else:
        return len(a)


def list_intersection(l1, l2):
    out = list(set(l1) & set(l2))
    if len(out) > 0:
        my_type = type(out[0])
        assert all(isinstance(x, my_type) for x in out)
    return out


def list_union(l1, l2):
    out = list(set(l1) | set(l2))
    if len(out) > 0:
        my_type = type(out[0])
        assert all(isinstance(x, my_type) for x in out)
    return out


def list_substract(l, l_substact):
    out = list(set(l) - set(l_substact))
    if len(out) > 0:
        my_type = type(out[0])
        assert all(isinstance(x, my_type) for x in out)
    return out


def tweet_type(tweet):
    if 'quoted_status' in tweet and isinstance(tweet['quoted_status'], dict):
        return Cte.QTWEET
    if 'retweeted_status' not in tweet:
        if tweet['in_reply_to_user_id_str'] is None:
            return Cte.TWEET
        else:
            return Cte.ANSWER

    elif isinstance(tweet['retweeted_status'], dict) and len(tweet['retweeted_status']) > 0:
        return Cte.RETWEET
    else:
        if tweet['in_reply_to_user_id_str'] is None:
            return Cte.TWEET
        else:
            return Cte.ANSWER


def tweet_type_twint(tweet):
    if len(tweet['reply_to']) > 1:
        return Cte.ANSWER
    else:

        return Cte.TWEET


def tweet_creator(tweet):
    output = {}
    if 'quoted_status' in tweet and isinstance(tweet['quoted_status'], dict):
        qtweet = tweet['quoted_status']
        output['id_str'] = qtweet['user']['id_str']
        output['id'] = int(qtweet['user']['id'])
        output['screen_name'] = qtweet['user']['screen_name']
        return output

    if 'retweeted_status' not in tweet:
        output['id_str'] = tweet['user']['id_str']
        output['id'] = int(tweet['user']['id'])
        output['screen_name'] = tweet['user']['screen_name']
    elif isinstance(tweet['retweeted_status'], dict) and len(tweet['retweeted_status']) > 0:
        output['id_str'] = tweet['retweeted_status']['user']['id_str']
        output['id'] = int(tweet['retweeted_status']['user']['id'])
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
    df['tweet_screen_name'] = df.apply(lambda x: tweet_creator(x)['screen_name'], axis=1)

    df['timestamp'] = df.apply(lambda x:
                               parser.parse(x['created_at']).timestamp(), axis=1)
    # df.sort_values(by='created_at', inplace=True)

    return df


def create_df_from_tweet_list_twint(tweet_list):
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

    df = df.drop(columns=['datestamp', 'timestamp'])
    df['timestamp'] = df.apply(lambda x: x['datetime'] / 1000, axis=1)
    df['datetime'] = df.apply(lambda x:
                              datetime.fromtimestamp(x['timestamp']), axis=1)

    df = df.rename(columns={'tweet': 'text',
                            'username': 'tweet_screen_name',
                            'tweet_user_id': 'user_id',
                            'tweet_user_id_str': 'user_id_str'})

    df['type'] = df.apply(lambda x: tweet_type_twint(x), axis=1)

    return df


def create_df_from_user_list(user_list, drop=True):
    if len(user_list) == 0:
        return pd.DataFrame(columns=['_id', 'id_str', 'contributors_enabled', 'created_at',
                                     'default_profile', 'default_profile_image', 'description', 'entities',
                                     'favourites_count', 'follow_request_sent', 'followers_count',
                                     'following', 'friends_count', 'geo_enabled', 'has_extended_profile',
                                     'id', 'is_translation_enabled', 'is_translator', 'lang', 'listed_count',
                                     'location', 'name', 'notifications', 'profile_background_color',
                                     'profile_background_image_url', 'profile_background_image_url_https',
                                     'profile_background_tile', 'profile_banner_url', 'profile_image_url',
                                     'profile_image_url_https', 'profile_link_color',
                                     'profile_sidebar_border_color', 'profile_sidebar_fill_color',
                                     'profile_text_color', 'profile_use_background_image', 'protected',
                                     'screen_name', 'status', 'statuses_count', 'time_zone',
                                     'translator_type', 'url', 'utc_offset', 'verified'])

    df = pd.DataFrame.from_dict(user_list)
    df.set_index(keys='id_str', inplace=True, drop=drop)

    return df


def clean_tweet(tweet):
    tweet_clean = {key: tweet[key] for key in
                   ['created_at', 'id', 'id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str',
                    'in_reply_to_screen_name',
                    'retweet_count', 'favorite_count', 'lang']}
    if 'full_text' in tweet.keys():
        tweet_clean['text'] = tweet['full_text']
    elif 'extended_tweet' in tweet.keys():
        tweet_clean['text'] = tweet['extended_tweet']['full_text']
    else:
        tweet_clean['text'] = tweet['text']
    if 'quote_count' in tweet.keys(): tweet_clean['quote_count'] = tweet['quote_count']
    if 'reply_count' in tweet.keys(): tweet_clean['reply_count'] = tweet['reply_count']
    tweet_clean['datetime'] = datetime.fromtimestamp(parser.parse(tweet['created_at']).timestamp())
    if 'type' not in tweet.keys(): tweet_clean['type'] = tweet_type(tweet)
    if 'tweet_user_id' not in tweet.keys(): tweet_clean['tweet_user_id'] = tweet_creator(tweet)['id']
    if 'tweet_user_id_str' not in tweet.keys(): tweet_clean['tweet_user_id_str'] = tweet_creator(tweet)['id_str']
    if 'tweet_user_screen_name' not in tweet.keys(): tweet_clean['tweet_user_screen_name'] = tweet_creator(tweet)[
        'screen_name']

    tweet_clean['timestamp'] = parser.parse(tweet['created_at']).timestamp()

    tweet_clean['text_processed'] = preprocess_text(tweet_clean['text'])
    text = tweetp.parse(tweet_clean['text'])
    tweet_clean['emojis'] = min(length(text.emojis), 127)
    tweet_clean['hashtags'] = min(length(text.hashtags), 127)
    tweet_clean['urls'] = min(length(text.urls), 127)
    tweet_clean['mentions'] = min(length(text.mentions), 127)
    return tweet_clean


def clean_list(list_):
    list_clean = list_.copy()
    user = None
    if 'user' in list_clean:
        user = list_clean['user'].copy()
        list_clean['user_id_str'] = list_clean['user']['id_str']
        list_clean['user_screen_name'] = list_clean['user']['screen_name']
        del list_clean['user']

    list_clean['created_at'] = parser.parse(list_clean['created_at']).replace(tzinfo=None)
    list_clean['text_processed'] = preprocess_text(' '.join([list_clean['name'], list_clean['description']]))

    return list_clean, user


def create_df_list(list_list):
    df = pd.DataFrame.from_dict(list_list)
    if len(df) == 0:
        return pd.DataFrame(columns=['_id', 'id_str', 'created_at', 'description', 'following', 'full_name',
                                     'id', 'member_count', 'mode', 'name', 'slug', 'subscriber_count', 'uri',
                                     'user_id_str', 'user_screen_name', 'datetime'])

    df['datetime'] = df.apply(lambda x: parser.parse(x['created_at']).replace(tzinfo=None), axis=1)
    return df


def create_df_from_cleaned_tweet_list(tweet_list):
    df = pd.DataFrame.from_dict(tweet_list)
    if len(df) == 0:
        return pd.DataFrame(columns=['created_at', 'id', 'id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str',
                                     'in_reply_to_screen_name',
                                     'text', 'quote_count', 'reply_count', 'retweet_count', 'favorite_count', 'lang',
                                     'datetime', 'type', 'tweet_user_id', 'tweet_user_id_str', 'tweet_user_screen_name',
                                     'timestamp'])

    df.set_index(keys='id_str', inplace=True)
    return df
