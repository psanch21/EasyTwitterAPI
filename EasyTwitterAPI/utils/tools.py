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
    elif 'retweeted_status' in tweet and isinstance(tweet['retweeted_status'], dict):
        return Cte.RETWEET
    elif  tweet['in_reply_to_user_id_str'] is not None:
        return Cte.ANSWER
    else:
        return Cte.TWEET


def tweet_type_twint(tweet):
    if len(tweet['reply_to']) > 1:
        return Cte.ANSWER
    else:

        return Cte.TWEET


def tweet_creator(tweet):
    output = {}

    type_ = tweet_type(tweet)
    if type_ == Cte.TWEET:
        output['tweet_user_id_str'] = tweet['user']['id_str']
        output['tweet_user_id'] = int(tweet['user']['id'])
        output['tweet_user_screen_name'] = tweet['user']['screen_name']
    elif type_ == Cte.RETWEET:
        output['tweet_user_id_str_interaction'] = tweet['retweeted_status']['user']['id_str']
        output['tweet_user_id_interaction'] = int(tweet['retweeted_status']['user']['id'])
        output['tweet_user_screen_name_interaction'] = tweet['retweeted_status']['user']['screen_name']
    elif type_ == Cte.QTWEET:
        qtweet = tweet['quoted_status']
        output['tweet_user_id_str_interaction'] = qtweet['user']['id_str']
        output['tweet_user_id_interaction'] = int(qtweet['user']['id'])
        output['tweet_user_screen_name_interaction'] = qtweet['user']['screen_name']
    elif type_ == Cte.ANSWER:
        output['tweet_user_id_str'] = tweet['user']['id_str']
        output['tweet_user_id'] = int(tweet['user']['id'])
        output['tweet_user_screen_name'] = tweet['user']['screen_name']

        output['tweet_user_id_str_interaction'] = tweet['in_reply_to_user_id_str']
        output['tweet_user_id_interaction'] = int(tweet['in_reply_to_user_id'])
        output['tweet_user_screen_name_interaction'] = tweet['in_reply_to_screen_name']
    else:
        raise NotImplementedError

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

def clean_tweet_no_timeline(tweet, preprocess):
    tweet_clean = {key: tweet[key] for key in
                   ['created_at', 'id', 'id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str',
                    'in_reply_to_screen_name', 'in_reply_to_status_id_str',
                    'retweet_count', 'favorite_count', 'lang']}

    if 'type' not in tweet.keys(): tweet_clean['type'] = tweet_type(tweet)

    tweet_clean['text_interaction'] = None
    tweet_clean['text'] = None

    tweet_clean['tweet_user_id_str'] = None
    tweet_clean['tweet_user_id'] = None
    tweet_clean['tweet_user_screen_name'] = None

    tweet_clean['tweet_user_id_str_interaction'] = None
    tweet_clean['tweet_user_id_interaction'] = None
    tweet_clean['tweet_user_screen_name_interaction'] = None

    creator = tweet_creator(tweet)
    tweet_clean.update(creator)

    if tweet_clean['type'] in [Cte.RETWEET]:
        tweet_clean['text_interaction'] = tweet['retweeted_status']['full_text']


    elif tweet_clean['type'] in [Cte.ANSWER]:
        tweet_clean['text'] = tweet['full_text']
        # TODO: tweet_clean['text_interaction']  in_reply_to_status_id_str

    elif tweet_clean['type'] in [Cte.TWEET]:
        tweet_clean['text'] = tweet['full_text']

    elif tweet_clean['type'] in [Cte.QTWEET]:
        tweet_clean['text'] = tweet['full_text']
        tweet_clean['text_interaction'] =   tweet['quoted_status']['full_text']
    else:
        raise NotImplementedError

    # if 'full_text' in tweet.keys():
    #     tweet_clean['text'] = tweet['full_text']
    # elif 'extended_tweet' in tweet.keys():
    #     tweet_clean['text'] = tweet['extended_tweet']['full_text']
    # else:
    #     tweet_clean['text'] = tweet['text']
    if 'quote_count' in tweet.keys(): tweet_clean['quote_count'] = tweet['quote_count']
    if 'reply_count' in tweet.keys(): tweet_clean['reply_count'] = tweet['reply_count']
    tweet_clean['datetime'] = datetime.fromtimestamp(parser.parse(tweet['created_at']).timestamp())

    tweet_clean['timestamp'] = parser.parse(tweet['created_at']).timestamp()

    if preprocess:
        tweet_clean['text_processed'] = preprocess_text(tweet_clean['text'])
    else:
        tweet_clean['text_processed'] = None

    text = ''
    if tweet_clean['text'] is not None:
        text = tweet_clean['text']
    if tweet_clean['text_interaction'] is not None:
        text += ' ' + tweet_clean['text_interaction']

    text = text.strip()
    text = tweetp.parse(text)
    tweet_clean['emojis'] = min(length(text.emojis), 127)
    tweet_clean['hashtags'] = min(length(text.hashtags), 127)
    tweet_clean['urls'] = min(length(text.urls), 127)
    tweet_clean['mentions'] = min(length(text.mentions), 127)

    return tweet_clean

def clean_tweet(tweet, preprocess, user_id_str):
    tweet_clean = clean_tweet_no_timeline(tweet, preprocess)

    tweet_clean['id_str_timeline'] = user_id_str

    return tweet_clean


def clean_list(list_, preprocess):
    list_clean = list_.copy()
    user = None
    if 'user' in list_clean:
        user = list_clean['user'].copy()
        list_clean['user_id_str'] = list_clean['user']['id_str']
        list_clean['user_screen_name'] = list_clean['user']['screen_name']
        del list_clean['user']

    list_clean['created_at'] = parser.parse(list_clean['created_at']).replace(tzinfo=None)
    if preprocess:
        list_clean['text_processed'] = preprocess_text(' '.join([list_clean['name'], list_clean['description']]))
    else:
        list_clean['text_processed'] = None

    return list_clean, user


def create_df_list(list_list):
    df = pd.DataFrame.from_dict(list_list)
    if len(df) == 0:
        return pd.DataFrame(columns=['_id', 'id_str', 'created_at', 'description', 'following', 'full_name',
                                     'id', 'member_count', 'mode', 'name', 'slug', 'subscriber_count', 'uri',
                                     'user_id_str', 'user_screen_name', 'datetime'])

    # df['datetime'] = df.apply(lambda x: parser.parse(x['created_at']).replace(tzinfo=None), axis=1)
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
