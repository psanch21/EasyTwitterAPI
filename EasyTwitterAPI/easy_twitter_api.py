import datetime
import json
import time
from datetime import timedelta

import numpy as np
import pandas as pd
from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterError
from dateutil import parser

import EasyTwitterAPI.utils.tools as utools
from EasyTwitterAPI.utils.constants import Cte

REPLIES_COL_TO_KEEP = ['_id', 'to_user_id_str', 'created_at', 'favorite_count', 'id',
                       'lang',
                       'reply_count', 'retweet_count',
                       'source', 'text', 'truncated', 'user',
                       'retweeted_status', 'extended_entities', 'datetime', 'type',
                       'tweet_user_id', 'tweet_user_id_str', 'tweet_screen_name', 'timestamp']

from EasyTwitterAPI.easy_twitter_db import EasyTwitterDB, DBCte


def get_credentials(cred_file):
    with open(cred_file) as cred_data:
        info = json.load(cred_data)
        consumer_key = info['CONSUMER_KEY']
        consumer_secret = info['CONSUMER_SECRET']
        access_key = info['ACCESS_KEY']
        access_secret = info['ACCESS_SECRET']

    return consumer_key, consumer_secret, access_key, access_secret


def print_user(user):
    # print(' | '.join([f'{k}: {v}' for k, v in user.items() if k != 'lists_m']))
    print(
        f"=> {user['screen_name']} | Tw: {user['statuses_count']} | Fav: {user['favourites_count']} "
        f" | L: {user['listed_count']} | B: {user['friends_count']} | F: {user['followers_count']}")


class EasyTwitterAPI:

    def __init__(self, cred_file,
                 db_name='easy_twitter_api',
                 sleep_secs=330,
                 host='localhost'
                 ):
        consumer_key, consumer_secret, access_token, access_token_secret = get_credentials(cred_file)

        self._cache = True  # Use local data if available

        self.alternative_cred_file_list = []
        self.pos_cred = 0

        self.api = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)
        self.api_premium = None
        self.premium_endpoint = None

        self.db = EasyTwitterDB(db_name=db_name, host=host)

        self.sleep_secs = sleep_secs

    def set_cred_premium(self, cred_file_premium, dev_label='awareness'):
        consumer_key, consumer_secret, access_token, access_token_secret = get_credentials(cred_file_premium)
        self.api_premium = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)
        self.premium_endpoint = f'tweets/search/fullarchive/:{dev_label}'

    def set_alternative_cred_file_list(self, file_list):
        self.alternative_cred_file_list = file_list
        self.pos_cred = np.random.randint(len(file_list))

    def activate_cache(self, value):
        self._cache = value

    def try_alternative_credentials(self):
        if len(self.alternative_cred_file_list) == 0: return

        cred_file = self.alternative_cred_file_list[self.pos_cred]
        self.pos_cred = (self.pos_cred + 1) % len(self.alternative_cred_file_list)

        print(f'Switching credentials: {cred_file}')
        consumer_key, consumer_secret, access_token, access_token_secret = get_credentials(cred_file)

        self.api = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)

    def try_request(self, endpoint, query, max_tries=7, premium=False):
        r = None

        error_list = [Cte.RATE_LIMIT, Cte.OVERLOAD, Cte.INTERNAL_ERROR, Cte.FORBIDDEN]
        error_list_not_handled = [Cte.UNAUTHORIZED, Cte.NOT_FOUND]
        while r is None:
            try:
                r = self.api.request(endpoint, query) if not premium else self.api_premium.request(endpoint, query)
            except TwitterError.TwitterConnectionError:
                print('Catching TwiTwitterConnectionError, sleeping for 10 secs')
                time.sleep(10)

        tries = 0
        while r.status_code in error_list and tries < max_tries:
            print(
                f'{tries} | Too many requests. Sleeping for  {self.sleep_secs} seconds... | {datetime.datetime.now()}')
            time.sleep(self.sleep_secs)
            print('AWAKE!')
            self.try_alternative_credentials()
            try:
                r = self.api.request(endpoint, query) if not premium else self.api_premium.request(endpoint, query)
            except TwitterError.TwitterConnectionError:
                print('Catching TwiTwitterConnectionError, sleeping for 10 secs')
                time.sleep(10)
            tries += 1

        if r.status_code in error_list_not_handled:
            return r, False

        return r, tries < max_tries

    # %% User activity

    def get_user_timeline(self, **args):

        assert self.premium_endpoint is not None

        count = args['count'] if 'count' in args else 500
        max_num = args['max_num'] if 'max_num' in args else None
        min_cache_tweets = args['min_cache_tweets'] if 'min_cache_tweets' in args else 1
        if max_num: count = min(count, max_num)
        toDate = args['toDate'] if 'toDate' in args else None
        fromDate = args['fromDate'] if 'fromDate' in args else None

        toDate_str = args['toDate'].strftime("%Y%m%d%H%m") if 'toDate' in args else None
        fromDate_str = args['fromDate'].strftime("%Y%m%d%H%m") if 'fromDate' in args else None
        preprocess = args['preprocess'] if 'preprocess' in args else False

        user = None

        if 'user_id' in args:
            user = self.get_user(user_id=args['user_id'])
        elif 'screen_name' in args:
            user = self.get_user(screen_name=args['screen_name'])
        elif 'user' in args:
            user = args['user']

        if user is None: return None

        if user['protected']: return None
        username, user_id = user['screen_name'], user['id']

        count = min(count, user['statuses_count'])
        collection_name = f'{TWEETS_U_C}{user_id}'

        print(f'Scraping {self.premium_endpoint} of {username} {user_id}')
        tweet_collection = self.db[collection_name]

        n_tweets = tweet_collection.find({}).count()
        if self._cache and n_tweets >= min_cache_tweets:
            df = utools.create_df_from_cleaned_tweet_list(tweet_collection.find({}))

            print(f'Getting user {user_id} activity from cache')
            print(f'{len(df)} events for user {user_id}')
            cols = list(df.columns)
            if 'user' not in cols and 'tweet_user_id_str' in cols:
                print('Getting statuses  cleaned')
                return df[df['timestamp'] > fromDate.timestamp()]
            elif 'user' not in cols and 'tweet_user_id_str' not in cols:
                print('Scrapping again...')
            else:
                df = pd.DataFrame.from_dict(tweet_collection.find({}))
                if '_id' in df.columns: df.drop(columns=['_id'], inplace=True)
                print('Updating to statuses v2')
                df = df.apply(lambda tweet: pd.Series(utools.clean_tweet(tweet, preprocess)), axis=1)
                self.db.drop_collection(collection_name)
                collection = self.db[collection_name]
                collection.insert_many(list(df.T.to_dict().values()))

                df = utools.create_df_from_cleaned_tweet_list(collection.find({}))
                return df[df['timestamp'] > fromDate.timestamp()]

        count_total = 0
        count_total_new = 0

        request = True

        query = {'query': f'from:{user_id}', 'maxResults': count}

        if fromDate: query['fromDate'] = fromDate_str
        if toDate: query['toDate'] = toDate_str

        while request:

            r, success = self.try_request(self.premium_endpoint, query, premium=True)
            if not success: return None

            count = 0

            for i, tweet in enumerate(r):
                count += 1
                count_total += 1
                tmp = tweet_collection.find_one_and_update({'id_str': tweet['id_str']},
                                                           {"$set": utools.clean_tweet(tweet, preprocess)},
                                                           upsert=True)
                if tmp is None: count_total_new += 1

                if max_num and count_total >= max_num:
                    request = False
                    break

            print(f'{count} tweets retrieved\n\n')
            if count > 0:
                tweet_timestamp = parser.parse(tweet['created_at']).timestamp()
                if tweet_timestamp < fromDate.timestamp():
                    request = False
                elif 'next' in r.json():
                    query['next'] = r.json()['next']
                else:
                    request = False

            else:
                request = False

        tweet_list = list(tweet_collection.find({}))
        print(f'{count_total_new} new events for user {user_id}')
        print(f'{len(tweet_list)} events for user {user_id}')

        df = utools.create_df_from_cleaned_tweet_list(tweet_list)
        return df[df['timestamp'] > fromDate.timestamp()]

    def get_user_activity_limited(self, **args):
        return self._get_user_activity_type(endpoint='statuses/user_timeline', **args)

    def update_user_activity(self, min_dt, **args):
        cache = self._cache
        self.activate_cache(True)
        df = self.get_user_activity_limited(**args)
        if df is None or len(df) == 0: return df
        if df['datetime'].max() < min_dt:
            self.activate_cache(False)
            id_max = df['datetime'].idxmax()
            df = self.get_user_activity_limited(since_id=id_max, **args)
            self.activate_cache(cache)

        return df

    def update_user_activity_premium(self, min_dt, fromDate, **args):

        df = self.update_user_activity(min_dt=min_dt,
                                       since=fromDate - timedelta(days=90),
                                       window=1, **args)
        if df is None or len(df) == 0: return df
        cache = self._cache
        self.activate_cache(True)
        cond = df['datetime'].min() > (fromDate + timedelta(days=1))
        count_cache = args['count_cache'] if 'count_cache' in args else 3100

        cond = cond and len(df) > count_cache
        if cond:
            self.activate_cache(False)
            df = self.get_user_timeline(fromDate=fromDate,
                                        toDate=df['datetime'].min(),
                                        **args)
            self.activate_cache(cache)

        if df is not None:
            return df[df['datetime'] >= fromDate]
        else:
            return df

    def _get_user_activity_type(self, endpoint, **args):

        count = args['count'] if 'count' in args else 200
        window = args['window'] if 'window' in args else 60
        max_num = args['max_num'] if 'max_num' in args else None
        min_cache_tweets = args['min_cache_tweets'] if 'min_cache_tweets' in args else 1
        if max_num: count = min(count, max_num)
        since = args['since'] if 'since' in args else datetime.datetime.strptime('2006-01-01', '%Y-%m-%d')
        preprocess = args['preprocess'] if 'preprocess' in args else False
        update_many = args['update_many'] if 'update_many' in args else True

        user = None

        if 'user_id' in args:
            user = self.get_user(user_id=args['user_id'])
        elif 'screen_name' in args:
            user = self.get_user(screen_name=args['screen_name'])
        elif 'user' in args:
            user = args['user']
        else:
            raise NotImplementedError

        if user is None: return None

        if user['protected']: return None
        username, user_id = user['screen_name'], user['id']
        user_id_str = user['id_str']

        if endpoint == 'statuses/user_timeline':
            count = min(count, user['statuses_count'])
            df = self.db.load_statuses_user(user_id_str=user_id_str,
                                            filter_={},
                                            return_as='df')
        else:
            count = min(count, user['favourites_count'])
            df = self.db.load_favourites_user(user_id_str=user_id_str,
                                              filter_={},
                                              return_as='df')

        print(f'Scraping {endpoint} of {username} {user_id}')

        n_tweets = len(df) if df is not None else 0
        since_id = args['since_id'] if 'since_id' in args else None

        if self._cache and n_tweets >= min_cache_tweets:
            # df = utools.create_df_from_cleaned_tweet_list(tweet_collection.find({}))

            print(f'Getting user {user_id} activity from cache')
            print(f'{len(df)} events for user {user_id}')
            cols = list(df.columns)

            using_cache = True
            if 'user' not in cols and 'tweet_user_id_str' not in cols:
                print('Scrapping again...')
                using_cache = False

            if using_cache:
                if count > (len(df) + 1) and len(df) < 3100 and df['datetime'].min() > (since + timedelta(days=1)):
                    print('Scrapping older tweets!')
                else:
                    return df[df['timestamp'] > since.timestamp()]

        count_total = 0
        count_total_new = 0

        request = True

        query = {'user_id': user_id, 'count': count, 'tweet_mode': 'extended'}
        if since_id: query['since_id'] = since_id

        min_timestamp = (since - timedelta(days=window)).timestamp()

        tweet_clean_list = []

        id_tweets_collected = [] if (df is None or len(df) == 0) else list(df['id_str'])
        while request:

            r, success = self.try_request(endpoint, query)
            if not success: return None

            count = 0

            for i, tweet in enumerate(r):
                count += 1
                count_total += 1
                tweet_clean = utools.clean_tweet(tweet, preprocess, user_id_str)
                if not update_many:
                    if endpoint == 'statuses/user_timeline':
                        tmp = self.db.update_statuses_user(filter_={'id_str': tweet['id_str'],
                                                                    'id_str_timeline': user_id_str},
                                                           data=tweet_clean)
                    else:
                        tmp = self.db.update_favourites_user(filter_={'id_str': tweet['id_str'],
                                                                      'id_str_timeline': user_id_str},
                                                             data=tweet_clean)

                else:
                    if id_tweets_collected.__contains__(tweet['id_str']):  # Tweet is already collected
                        tmp = 'Not None'
                    else:
                        tmp = None
                        tweet_clean = utools.clean_tweet(tweet, preprocess, user_id_str)
                        tweet_clean_list.append(tweet_clean)

                if tmp is None: count_total_new += 1

                if max_num and count_total >= max_num:
                    request = False
                    break

            print(f'{count} tweets retrieved\n\n')
            if count > 0:
                tweet_timestamp = parser.parse(tweet['created_at']).timestamp()
                if tweet_timestamp < min_timestamp:
                    request = False
                else:
                    query['max_id'] = tweet['id'] - 1
            else:
                request = False
        print(f'{count_total_new} new events for user {user_id}')
        if update_many:
            if endpoint == 'statuses/user_timeline':
                self.db.insert_many_statuses_user(tweet_clean_list, id_str_user=user_id_str)
            else:
                self.db.insert_many_favourites(tweet_clean_list, id_str_user=user_id_str)

        if endpoint == 'statuses/user_timeline':
            df = self.db.load_statuses_user(user_id_str=user_id_str,
                                            filter_={},
                                            return_as='df')
        else:
            df = self.db.load_favourites_user(user_id_str=user_id_str,
                                              filter_={},
                                              return_as='df')

        print(f'{len(df)} events for user {user_id}')

        # df = utools.create_df_from_cleaned_tweet_list(tweet_list)
        if len(df) > 0:
            return df[df['timestamp'] > since.timestamp()]
        else:
            return df

    def get_tweets(self, id_str_list, **args):
        assert all([isinstance(id_, str) for id_ in id_str_list])

        preprocess = args['preprocess'] if 'preprocess' in args else False
        update_many = args['update_many'] if 'update_many' in args else True

        endpoint = f'statuses/lookup'
        df = self.db.load_statuses(tweet_id_str_list=id_str_list,
                                   filter_={},
                                   return_as='df')

        print(f'Scraping  {len(id_str_list)} entries using {endpoint}')

        n_tweets = len(df) if df is not None else 0

        if self._cache and n_tweets >= len(id_str_list):
            # df = utools.create_df_from_cleaned_tweet_list(tweet_collection.find({}))

            print(f'Getting {len(id_str_list)}  tweets from cache')
            return df

        if df is not None and len(df) > 0:
            id_str_list_to_collect = utools.list_substract(id_str_list, list(df['id_str'].values))
        else:
            id_str_list_to_collect = id_str_list

        n_batches = int(np.ceil(len(id_str_list_to_collect) / 100))
        count_total = 0

        tweet_clean_list = []

        for i in range(n_batches):
            count = 0
            id_str_list_to_collect_i = id_str_list_to_collect[i * 100:(i + 1) * 100]

            query = {'id': ','.join(id_str_list_to_collect_i), 'tweet_mode': 'extended'}

            r, success = self.try_request(endpoint, query)
            if not success: return None

            for i, tweet in enumerate(r):
                count_total += 1
                count += 1
                tweet_clean = utools.clean_tweet_no_timeline(tweet, preprocess)
                if not update_many:
                    _ = self.db.update_statuses(filter_={'id_str': tweet['id_str']},
                                                data=tweet_clean)


                else:

                    tweet_clean = utools.clean_tweet_no_timeline(tweet, preprocess)
                    tweet_clean_list.append(tweet_clean)

                if len(id_str_list_to_collect) == count_total:
                    break

            print(f'{count} tweets retrieved\n\n')
        print(f'{count_total} new events')
        if update_many:
            self.db.insert_many_statuses(tweet_clean_list)

        df = self.db.load_statuses(tweet_id_str_list=id_str_list,
                                   filter_={},
                                   return_as='df')

        print(f'{len(df)} tweets')

        return df

    # %% User Favorites
    def get_user_favorites(self, **args):
        return self._get_user_activity_type(endpoint='favorites/list', **args)

    def update_user_favorites(self, min_dt, **args):
        cache = self._cache
        self.activate_cache(True)
        df_favs = self.get_user_favorites(**args)
        if df_favs is None or len(df_favs) == 0: return df_favs
        if df_favs['datetime'].max() < min_dt:
            self.activate_cache(False)
            id_max = df_favs['datetime'].idxmax()
            df_favs = self.get_user_favorites(since_id=id_max, **args)
            self.activate_cache(cache)

        return df_favs

    def get_user_favorites_2(self, **args):

        count = args['count'] if 'count' in args else 200
        max_num = args['max_num'] if 'max_num' in args else None
        min_cache_tweets = args['min_cache_tweets'] if 'min_cache_tweets' in args else 1
        if max_num: count = min(count, max_num)
        since = args['since'] if 'since' in args else datetime.datetime.strptime('2006-01-01', '%Y-%m-%d')

        if 'user_id' in args:
            user = self.get_user(user_id=args['user_id'])
        elif 'screen_name' in args:
            user = self.get_user(screen_name=args['screen_name'])
        elif 'user' in args:
            user = args['user']

        if user['protected']: return None
        username, user_id = user['screen_name'], user['id']

        count = min(count, user['favourites_count'])
        print(f'Scraping user activity of {username} {user_id}')

        tweet_collection = self.db[f'favs_{user_id}']

        n_tweets = tweet_collection.find({}).count()

        if self._cache and n_tweets >= min_cache_tweets:
            tweet_list = list(tweet_collection.find({}))
            print(f'Getting user {user_id} activity from cache')
            print(f'{len(tweet_list)} liked tw for user {user_id}')

            df = utools.create_df_from_tweet_list(tweet_list)
            return df[df['timestamp'] > since.timestamp()]

        count_total = 0
        count_total_new = 0

        request = True

        query = {'user_id': user_id, 'count': count}

        endpoint = 'favorites/list'
        while request:

            r, success = self.try_request(endpoint, query)
            if not success: return None

            count = 0

            for i, tweet in enumerate(r):
                count += 1
                count_total += 1
                tmp = tweet_collection.find_one_and_update({'id_str': tweet['id_str']},
                                                           {"$set": tweet},
                                                           upsert=True)
                if tmp is None: count_total_new += 1

                if max_num and count_total >= max_num:
                    request = False
                    break

            print(f'{count} liked tweets retrieved\n\n')
            if count > 0:
                tweet_timestamp = parser.parse(tweet['created_at']).timestamp()
                if tweet_timestamp < since.timestamp():
                    request = False
                else:
                    query['max_id'] = tweet['id'] - 1
            else:
                request = False

        tweet_list = list(tweet_collection.find({}))
        print(f'{count_total_new} new events for user {user_id}')
        print(f'{len(tweet_list)} events for user {user_id}')

        df = utools.create_df_from_tweet_list(tweet_list)
        return df[df['timestamp'] > since.timestamp()]

    # %% Connections

    def get_followees(self, **args):

        return self.get_connection(connection_type=Cte.FRIENDS, **args)

    def get_followees_cache(self, user_id):
        list_name = f'{Cte.FRIENDS}_list'

        data = self.db[list_name].find_one({"id_str": user_id})
        ids_list = [str(i) for i in data['id_str_list']]
        return ids_list

    def get_followers(self, **args):

        return self.get_connection(connection_type=Cte.FOLLOWERS, **args)

    def get_connection(self, connection_type, **args):
        assert connection_type in [Cte.FOLLOWERS, Cte.FRIENDS]

        user = None

        if 'screen_name' in args:
            user = self.get_user(screen_name=args['screen_name'])
        elif 'user_id' in args:
            user = self.get_user(user_id=args['user_id'])
        elif 'user' in args:
            user = args['user']

        max_num = args['max_num'] if 'max_num' in args else 1000000
        str_format = args['str_format'] if 'str_format' in args else True
        cache = args['cache'] if 'cache' in args else self._cache
        username, user_id = user['screen_name'], str(user['id'])
        true_count = user['friends_count'] if connection_type == Cte.FRIENDS else user['followers_count']

        print(f'Scraping {connection_type} of user {username} {user_id}')
        find_query = {"id_str": user['id_str']}

        collection_name = f'{connection_type}_list'

        data = self.db.load(collection=collection_name,
                            filter_=find_query,
                            find_one=True)

        data = data if data else {'id_str': user['id_str'], 'id_str_list': []}

        if cache and len(data['id_str_list']) > 0:
            ids_list = [str(i) for i in data['id_str_list']] if str_format else data['id_str_list']

            print(f'Getting user {user_id} {connection_type} from cache')
            print(f'{len(ids_list)} {connection_type} for user {user_id}')
            if len(ids_list) >= true_count: return ids_list

        request = True
        endpoint = f'{connection_type}/ids'
        query = {'user_id': user_id, 'count': 5000}
        total_count = 0
        while request:

            r, success = self.try_request(endpoint, query)
            if not success: return []

            count = 0
            for i, u_id in enumerate(r):
                data['id_str_list'].append(u_id)
                count += 1
                total_count += 1
                if total_count >= max_num:
                    request = False
                    break

            print(f'{count} users retrieved\n\n')

            if 'next_cursor' in r.json() and r.json()['next_cursor'] != 0:
                query['cursor'] = r.json()['next_cursor']
            else:
                request = False

            data['id_str_list'] = list(set(data['id_str_list']))

            self.db.update_data(collection=collection_name,
                                filter_={"id_str": user['id_str']},
                                data=data)

        output = [str(i) for i in data['id_str_list']] if str_format else data['id_str_list']
        return output  # list(users_collection.find({"id": {"$in": user[list_name]}}))

    def get_relationship(self, **args):

        query = {}
        find_query = {}
        user_source, user_target = None, None
        if 'source_id' in args:
            source_id = args['source_id']
            query['source_id'] = source_id
            find_query['source_id'] = source_id
            user_source = self.get_user(user_id=source_id)

        elif 'source_screen_name' in args:
            source_screen_name = args['source_screen_name']
            query['source_screen_name'] = source_screen_name
            user_source = self.get_user(screen_name=source_screen_name)
            find_query['source_id'] = user_source['id']
        elif 'source' in args:
            user_source = args['source']
            source_screen_name = user_source['screen_name']
            query['source_screen_name'] = source_screen_name
            find_query['source_id'] = user_source['id']

        if 'target_id' in args:
            target_id = args['target_id']
            query['target_id'] = target_id
            find_query['target_id'] = target_id
            user_target = self.get_user(user_id=target_id)

        elif 'target_screen_name' in args:
            target_screen_name = args['target_screen_name']
            query['target_screen_name'] = target_screen_name
            user_target = self.get_user(screen_name=target_screen_name)
            find_query['target_id'] = user_target['id']
        elif 'target' in args:
            user_target = args['target']
            target_screen_name = user_target['screen_name']
            query['target_screen_name'] = target_screen_name
            find_query['target_id'] = user_target['id']

        rel_collection = self.db[RELAT_C]

        count_cache = rel_collection.find(find_query).count()

        if self._cache and count_cache > 0:
            rel_data = rel_collection.find_one(find_query)
            print(
                f"Getting relation between source {user_source['screen_name']} and target {user_target['screen_name']} from cache")
            return rel_data

        request = True
        endpoint = 'friendships/show'
        while request:

            r, success = self.try_request(endpoint, query)
            if not success: return None

            count = 0
            for i, relation in enumerate(r):
                rel_collection.find_one_and_update(find_query,
                                                   {"$set": relation['relationship']},
                                                   upsert=True)
                count += 1

            print(f'{count} relationships retrieved\n\n')

            request = False

        return rel_collection.find_one(find_query)

    # %% User profile

    def get_many_users(self, **args):

        drop = args['drop'] if 'drop' in args else True
        if 'screen_name' in args:
            user_id_list_total = args['screen_name']
            filter_ = {'screen_name': {'$in': user_id_list_total}}

        elif 'user_id' in args:
            user_id_list_total = [str(i) for i in args['user_id']]
            filter_ = {'id_str': {'$in': user_id_list_total}}
        else:
            raise NotImplementedError

        df = self.db.load_users(filter_=filter_,
                                find_one=False,
                                return_as='df',
                                drop=drop)
        if len(df) == len(user_id_list_total):
            return df

        user_list = []

        if 'screen_name' in args:
            if len(df) > 0:
                user_id_list = list(set(user_id_list_total) - set(df['screen_name'].unique()))
            else:
                user_id_list = list(set(user_id_list_total))

        elif 'user_id' in args:
            if len(df) > 0:
                user_id_list = list(set(user_id_list_total) - set(df['id_str'].unique()))
            else:
                user_id_list = list(set(user_id_list_total))

        n_iter = np.ceil(len(user_id_list) / 100).astype(int)

        for i in range(n_iter):
            if 'screen_name' in args:
                u = self.get_user(screen_name=user_id_list[i * 100:(i + 1) * 100])
                if not isinstance(u, list): u = [u]
                user_list.extend(u)
            elif 'user_id' in args:
                u = self.get_user(user_id=user_id_list[i * 100:(i + 1) * 100])
                if not isinstance(u, list): u = [u]
                user_list.extend(u)
        df = self.db.load_users(filter_=filter_,
                                find_one=False,
                                return_as='df',
                                drop=drop)
        return df

    def get_user(self, **args):
        '''
        This method returns a  user or list of users
        :param args:
        :return: user (json_format)
        '''

        if 'screen_name' in args:
            id_list = args['screen_name'] if isinstance(args['screen_name'], list) else [args['screen_name']]
            query = {'screen_name': ','.join(id_list)}
            find_query = {"screen_name": {"$in": id_list}}

        elif 'user_id' in args:
            id_list = args['user_id'] if isinstance(args['user_id'], list) else [args['user_id']]
            id_list = [str(i) for i in id_list]
            query = {'user_id': ','.join(id_list)}
            find_query = {"id_str": {"$in": id_list}}
        else:
            raise NotImplementedError

        count_cache = self.db.count(DBCte.USER_C, find_query)

        if self._cache and count_cache == len(id_list):
            user_list = self.db.load(collection=DBCte.USER_C,
                                     filter_=find_query,
                                     return_as='list')

            for user in user_list:
                print(f"Getting user {user['id']} from cache")
                print_user(user)

            if len(id_list) == 1:
                return user_list[0]
            else:
                return user_list

        request = True
        endpoint = 'users/lookup'
        user_list = []
        while request:

            r, success = self.try_request(endpoint, query)
            if not success: return None

            count = 0
            for i, user in enumerate(r):
                user = self.preprocess_user(user)
                self.db.update_data(collection=DBCte.USER_C,
                                    filter_={'id_str': user['id_str']},
                                    data=user)
                user_list.append(user)
                count += 1
                print_user(user=user)

            print(f'{count} users retrieved\n\n')

            if 'next_cursor' in r.json() and r.json()['next_cursor'] != 0:
                query['cursor'] = r.json()['next_cursor']
            else:
                request = False

        if len(id_list) == 1:
            return user_list[0]
        else:
            return user_list

    def preprocess_user(self, user):
        keys = list(user.keys())
        for k in keys:
            if 'profile' in k:
                user.pop(k)
        return user

    # %% Lists

    def get_list(self, list_id_str, **args):

        list_ = self.db.load_lists(filter_={'id_str': list_id_str},
                                   find_one=True)

        preprocess = args['preprocess'] if 'preprocess' in args else False

        if self._cache and list_:
            print(f"Getting List {list_['id_str']}  from cache")
            return list_

        endpoint = 'lists/show'

        request = True

        query = {'list_id': list_id_str}
        total_count, count_total_new = 0, 0
        while request:

            r, success = self.try_request(endpoint, query)
            if not success: return None

            count = 0
            for i, l in enumerate(r):
                l_clean, user_ = utools.clean_list(l, preprocess)

                tmp = self.db.update_list(filter_={'id_str': l['id_str']},
                                          data=l_clean)

                self.db.update_user(filter_={'id_str': user_['id_str']},
                                    data=user_)
                count += 1
                total_count += 1

                if tmp is None: count_total_new += 1

            assert count == 1
            request = False
            print(f'{count} lists retrieved')

        print(f'{count_total_new} new List')

        return l

    def get_lists_of_user_full(self, list_type, **args):
        l_id_str_list = self.get_lists_ids_of_user(list_type, **args)
        if l_id_str_list is None: l_id_str_list = []
        cursor = self.db.load_lists(filter_={'id_str': {'$in': l_id_str_list}},
                                    return_as='cursor')
        df_lists = utools.create_df_list(cursor)
        if len(df_lists) == len(l_id_str_list): return df_lists
        l_remaining = set(l_id_str_list) - set(df_lists['id_str'].unique())
        for id_str in list(l_remaining):
            l = self.get_list(list_id_str=id_str)

        cursor = self.db.load_lists(filter_={'id_str': {'$in': l_id_str_list}},
                                    return_as='cursor')
        df_lists = utools.create_df_list(cursor)
        assert len(df_lists) == len(l_id_str_list)

        return df_lists

    def get_lists_ids_of_user(self, list_type, **args):
        '''
        https://developer.twitter.com/en/docs/accounts-and-users/create-manage-lists/api-reference/get-lists-memberships

        This method returns a list of Twitter Lists
        :param list_type: own (o), membership (m) or  subsriptions (s)
        :param args:
        :return:
        '''

        user = None

        count = args['count'] if 'count' in args else 1000
        max_num = args['max_num'] if 'max_num' in args else 100000
        force = args['force'] if 'force' in args else False
        since = args['since'] if 'since' in args else datetime.datetime.strptime('2009-01-01', '%Y-%m-%d')
        preprocess = args['preprocess'] if 'preprocess' in args else False
        if 'screen_name' in args:
            user = self.get_user(screen_name=args['screen_name'])
        elif 'user_id' in args:
            user = self.get_user(user_id=args['user_id'])
        elif 'user' in args:
            user = args['user']

        username, user_id = user['screen_name'], str(user['id'])
        print(f'Scraping Lists of {username} {user_id}')

        data = self.db.load_lists_of_user(list_type=list_type,
                                          filter_={"id_str": user['id_str']},
                                          find_one=True)

        data = data if data else {'id_str': user['id_str'], 'lists': []}

        if not force and self._cache and len(data['lists']) > 0:
            print(f'Getting user {user_id} {list_type} from cache')
            lists_user = data['lists']
            print(f'{len(lists_user)} Lists for user {user_id}')
            return lists_user[:max_num]

        endpoint = None
        if list_type == Cte.LIST_MEMBER:
            endpoint = 'lists/memberships'
        elif list_type == Cte.LIST_OWN:
            endpoint = 'lists/ownerships'
        elif list_type == Cte.LIST_SUSC:
            endpoint = 'lists/subscriptions'

        request = True

        count = min(count, max_num)

        query = {'user_id': user_id, 'count': count}
        # users_collection = self.db[f'users']
        total_count, count_total_new = 0, 0

        while request:

            r, success = self.try_request(endpoint, query)
            if not success: return None

            count = 0
            for i, json in enumerate(r):
                for l in json['lists']:
                    l_clean, user_ = utools.clean_list(l, preprocess)
                    tmp = self.db.update_list(filter_={'id_str': l['id_str']},
                                              data=l_clean)

                    self.db.update_user(filter_={'id_str': user_['id_str']},
                                        data=user_)

                    data['lists'].append(l['id_str'])
                    count += 1
                    total_count += 1
                    if parser.parse(l['created_at']).replace(tzinfo=None) < since:
                        request = False
                        break
                    if tmp is None: count_total_new += 1

                    if total_count > max_num:
                        request = False
                        break
            print(f'{count} lists retrieved')

            if 'next_cursor' in r.json() and r.json()['next_cursor'] != 0:
                query['cursor'] = r.json()['next_cursor']
            else:
                request = False

            data['lists'] = list(set(data['lists']))

            self.db.update_lists_of_user(list_type=list_type,
                                         filter_={'id_str': user['id_str']},
                                         data=data)

        print(f'{count_total_new} new Lists for user {user_id}')
        print(f"{len(data['lists'])} Lists for user {user_id}")

        return data['lists'][:max_num]

    def update_lists_of_user(self, list_type, min_dt, **args):
        cache = self._cache
        self.activate_cache(True)
        df = self.get_lists_of_user_full(list_type, **args)
        if df is None or len(df) == 0: return df
        if df['created_at'].max() < min_dt:
            self.activate_cache(False)
            _ = self.get_lists_ids_of_user(list_type=list_type, since=df['created_at'].max(), **args)
            self.activate_cache(cache)

        full = args['full'] if 'full' in args else False
        if full:
            df = self.get_lists_of_user_full(list_type, **args)
        else:
            df = self.get_lists_ids_of_user(list_type, **args)
        return df

    def get_members_of_list(self, list_id_str, **args):

        # my_list = self.get_list(list_id_str=list_id_str)
        count = args['count'] if 'count' in args else 1000
        force = args['force'] if 'force' in args else False
        max_num = args['max_num'] if 'max_num' in args else 100000
        cache = args['cache'] if 'cache' in args else self._cache

        data = self.db.load_members_of_lists(filter_={'id_str': list_id_str},
                                             find_one=True)

        data = data if data else {'id_str': list_id_str, 'member_list': []}

        if isinstance(data['member_list'], int):
            if not force:
                error = data['member_list']
                print(f'ERROR in members of List {list_id_str}: {error}')
                return []
            else:
                data['member_list'] = []
        if cache and len(data['member_list']) > 0:
            print(f'Getting members of List {list_id_str}  from cache')
            return data['member_list']

        request = True
        endpoint = f'lists/members'
        query = {'list_id': list_id_str, 'count': count}

        total_count = 0

        while request:

            r, success = self.try_request(endpoint, query)
            if not success:
                data['member_list'] = r.status_code
                self.db.update_members_of_list(filter_={'id_str': list_id_str},
                                               data=data)
                return []

            count = 0
            for i, u in enumerate(r):
                data['member_list'].append(u['id'])
                print_user(user=u)
                # users_collection.find_one_and_update({'id': u['id']},
                #                                      {"$set": u},
                #                                      upsert=True)
                count += 1
                total_count += 1
                if total_count >= max_num:
                    request = False
                    break

            print(f'{count} users retrieved\n\n')

            if 'next_cursor' in r.json() and r.json()['next_cursor'] != 0:
                query['cursor'] = r.json()['next_cursor']
            else:
                request = False

            data['member_list'] = list(set(data['member_list']))
            self.db.update_members_of_list(filter_={'id_str': list_id_str},
                                           data=data)

        return data['member_list']

    def search_replies_to(self, **args):
        '''
        This method users the Twitter API
        :param args:
        :return:
        '''

        # TODO: Not Implemented
        raise NotImplementedError

        day = timedelta(days=1)

        max_results = max(10, min(args['max_results'], 100)) if 'max_results' in args else 10

        toDate = args['toDate'] if 'toDate' in args else None
        fromDate = args['fromDate'] if 'fromDate' in args else None

        toDate_str = args['toDate'].strftime("%Y%m%d%H%m") if 'toDate' in args else None
        fromDate_str = args['fromDate'].strftime("%Y%m%d%H%m") if 'fromDate' in args else None

        if 'screen_name' in args:
            user = self.get_user(screen_name=args['screen_name'])
        elif 'user_id' in args:
            user = self.get_user(user_id=args['user_id'])
        elif 'user' in args:
            user = args['user']

        screen_name, user_id_str = user['screen_name'], user['id_str']

        print(f'Scraping replies to {screen_name}')

        tweets_collection = self.db[TWEETS_V2]

        from_user = None
        if 'from_screen_name' in args:
            from_user = self.get_user(screen_name=args['from_screen_name'])
        elif 'from_user_id' in args:
            from_user = self.get_user(user_id=args['from_user_id'])
        elif 'from_user' in args:
            from_user = args['from_user']

        query_id = {'in_reply_to_user_id_str': user_id_str,
                    'tweet_user_id_str': from_user['id_str']} if from_user else {'in_reply_to_user_id_str': user_id_str}

        n_tweets = tweets_collection.count_documents(query_id)

        if self._cache and n_tweets > 0:
            tweet_list = list(tweets_collection.find(query_id))

            print(f'Getting replies to  {screen_name} from cache')
            print(f'{len(tweet_list)} events for user {screen_name}')

            df = utools.create_df_from_cleaned_tweet_list(tweet_list)
            if toDate:  df = df[df['timestamp'] <= (toDate + day).timestamp()]
            if fromDate:  df = df[df['timestamp'] >= (fromDate - day).timestamp()]

            if len(df) > 0: return df

        count_total_new = 0
        count = 0

        query = f"to:{screen_name} from:{from_user['screen_name']}" if from_user else f'to:{screen_name}'

        query = {'query': query, 'maxResults': max_results}
        if toDate_str: query['toDate'] = toDate_str
        if fromDate_str: query['fromDate'] = fromDate_str

        endpoint = f'tweets/search/fullarchive/:prod'

        r, success = self.try_request(endpoint, query, premium=True)
        if not success: return None

        for i, tweet in enumerate(r):
            tweet_cleaned = utools.clean_tweet(tweet, preprocess)

            tmp = self.save_entry(data=tweet_cleaned,
                                  id={'id_str': tweet_cleaned['id_str']},
                                  collection=tweets_collection)
            count += 1
            if tmp is None: count_total_new += 1

            print(f'{count} tweets retrieved\n\n')

        tweet_list = list(tweets_collection.find(query_id))

        print(f'{count_total_new} new events for user {user_id_str}')
        print(f'{len(tweet_list)} events for user {user_id_str}')

        df = utools.create_df_from_cleaned_tweet_list(tweet_list)
        if toDate:  df = df[df['timestamp'] <= (toDate + day).timestamp()]
        if fromDate:  df = df[df['timestamp'] >= (fromDate - day).timestamp()]
        return df

    def search_premium(self, query, **args):
        # TODO: Not Implemented
        raise NotImplementedError

        day = timedelta(days=1)

        max_results = max(10, min(args['max_results'], 100)) if 'max_results' in args else 10

        toDate = args['toDate'] if 'toDate' in args else None
        fromDate = args['fromDate'] if 'fromDate' in args else None

        toDate_str = args['toDate'].strftime("%Y%m%d%H%m") if 'toDate' in args else None
        fromDate_str = args['fromDate'].strftime("%Y%m%d%H%m") if 'fromDate' in args else None

        if 'screen_name' in args:
            user = self.get_user(screen_name=args['screen_name'])
        elif 'user_id' in args:
            user = self.get_user(user_id=args['user_id'])
        elif 'user' in args:
            user = args['user']

        screen_name, user_id_str = user['screen_name'], user['id_str']

        print(f'Scraping replies to {screen_name}')

        tweets_collection = self.db[TWEETS_V2]

        from_user = None
        if 'from_screen_name' in args:
            from_user = self.get_user(screen_name=args['from_screen_name'])
        elif 'from_user_id' in args:
            from_user = self.get_user(user_id=args['from_user_id'])
        elif 'from_user' in args:
            from_user = args['from_user']

        query_id = {'in_reply_to_user_id_str': user_id_str,
                    'tweet_user_id_str': from_user['id_str']} if from_user else {'in_reply_to_user_id_str': user_id_str}

        n_tweets = tweets_collection.count_documents(query_id)

        if self._cache and n_tweets > 0:
            tweet_list = list(tweets_collection.find(query_id))

            print(f'Getting replies to  {screen_name} from cache')
            print(f'{len(tweet_list)} events for user {screen_name}')

            df = utools.create_df_from_cleaned_tweet_list(tweet_list)
            if toDate:  df = df[df['timestamp'] <= (toDate + day).timestamp()]
            if fromDate:  df = df[df['timestamp'] >= (fromDate - day).timestamp()]

            if len(df) > 0: return df

        count_total_new = 0
        count = 0

        query = f"to:{screen_name} from:{from_user['screen_name']}" if from_user else f'to:{screen_name}'

        query = {'query': query, 'maxResults': max_results}
        if toDate_str: query['toDate'] = toDate_str
        if fromDate_str: query['fromDate'] = fromDate_str

        endpoint = f'tweets/search/fullarchive/:prod'

        r, success = self.try_request(endpoint, query, premium=True)
        if not success: return None

        for i, tweet in enumerate(r):
            tweet_cleaned = utools.clean_tweet(tweet, preprocess)

            tmp = self.save_entry(data=tweet_cleaned,
                                  id={'id_str': tweet_cleaned['id_str']},
                                  collection=tweets_collection)
            count += 1
            if tmp is None: count_total_new += 1

            print(f'{count} tweets retrieved\n\n')

        tweet_list = list(tweets_collection.find(query_id))

        print(f'{count_total_new} new events for user {user_id_str}')
        print(f'{len(tweet_list)} events for user {user_id_str}')

        df = utools.create_df_from_cleaned_tweet_list(tweet_list)
        if toDate:  df = df[df['timestamp'] <= (toDate + day).timestamp()]
        if fromDate:  df = df[df['timestamp'] >= (fromDate - day).timestamp()]
        return df
