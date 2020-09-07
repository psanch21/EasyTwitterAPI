import datetime
import json
import time

import numpy as np
import twint
from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterError
from dateutil import parser
from pymongo import MongoClient
import random


import EasyTwitterAPI.utils.tools as utools
from EasyTwitterAPI.utils.constants import Cte

LIST_C = 'list'
LIST_MEMBERS_C = 'list_members'
LIST_MEMSHIP_C = 'list_m'
USER_FRIENDS = f'{Cte.FRIENDS}_list'

USER_FERS = f'{Cte.FOLLOWERS}_list'
USER_C = 'user'
TWEETS_U_C = 'tweets_'
REPLYTO_C = 'replies_'

FAVS_U_C = 'favs_'
RELAT_C = 'relationship'


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
        f"=> {user['screen_name']} | Tw: {user['statuses_count']} | Fav: {user['favourites_count']}  | L: {user['listed_count']} | Fr: {user['friends_count']}")


class EasyTwitterAPI:

    def __init__(self, cred_file, db_name='easy_twitter_api'):
        consumer_key, consumer_secret, access_token, access_token_secret = get_credentials(cred_file)

        self._cache = True  # Use local data if available

        self.alternative_cred_file_list = []

        self.api = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)

        self.db = MongoClient('localhost', 27017)[db_name]


    def set_alternative_cred_file_list(self, file_list):
        self.alternative_cred_file_list = file_list


    def activate_cache(self, value):
        self._cache = value

    def info_db(self, full=False):
        users = self.db[USER_C]
        lists = self.db[LIST_C]

        tweets_coll_list = [i for i in self.db.list_collection_names() if TWEETS_U_C in i]
        favs_coll_list = [i for i in self.db.list_collection_names() if FAVS_U_C in i]
        replies_coll_list = [i for i in self.db.list_collection_names() if REPLYTO_C in i]
        print(f'# users: {users.find({}).count()}')
        print(f'# lists: {lists.find({}).count()}')
        print(f'# users with activity: {len(tweets_coll_list)}')
        print(f'# users with favs: {len(favs_coll_list)}')
        print(f'# users with followees: {self.db[USER_FRIENDS].count_documents({})}')
        print(f'# users with followers: {self.db[USER_FERS].count_documents({})}')
        print(f'# users with lists membership: {self.db[LIST_MEMSHIP_C].count_documents({})}')
        print(f'# lists with  members: {self.db[LIST_MEMBERS_C].count_documents({})}')
        print(f'# relationships: {self.db[RELAT_C].count_documents({})}')
        print(f'# users with replies: {len(replies_coll_list)}')

        if not full: return
        count = 0
        for tw_coll in tweets_coll_list:
            count += self.db[tw_coll].find({}).count()
        print(f'# tweets: {count}')

        count = 0
        for tw_coll in favs_coll_list:
            count += self.db[tw_coll].find({}).count()
        print(f'# favs: {count}')

    def load_cache_data(self, collection, filter_={}, find_one=False):
        if collection not in self.db.list_collection_names():
            return None

        if find_one:
            return self.db[collection].find_one(filter_)
        else:
            return self.db[collection].find(filter_)

    def update_cache_data(self, collection, filter_, data):
        assert collection in self.db.list_collection_names()

        tmp = self.db[collection].find_one_and_update(filter_,
                                                      {"$set": data},
                                                      upsert=True)



    def try_alternative_credentials(self):
        if len(self.alternative_cred_file_list) == 0: return

        cred_file = random.choice(self.alternative_cred_file_list)
        print(f'Switching credentials: {cred_file}')
        consumer_key, consumer_secret, access_token, access_token_secret = get_credentials(cred_file)

        self.api = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)


    def try_request(self, endpoint, query, max_tries=16, sleep=180):
        r = None

        while r is None:
            try:
                r = self.api.request(endpoint, query)
            except TwitterError.TwitterConnectionError:
                print('Catching TwiTwitterConnectionError, sleeping for 10 secs')
                time.sleep(10)

        tries = 0
        while r.status_code in [Cte.RATE_LIMIT, Cte.OVERLOAD, Cte.INTERNAL_ERROR] and tries < max_tries:
            print(f'{tries} | Too many requests. Sleeping for a {sleep} seconds... | {datetime.datetime.now()}')
            time.sleep(sleep)
            print('AWAKE!')
            if tries % 2 == 0: self.try_alternative_credentials()
            try:
                r = self.api.request(endpoint, query)
            except TwitterError.TwitterConnectionError:
                print('Catching TwiTwitterConnectionError, sleeping for 10 secs')
                time.sleep(10)
            tries += 1

        if r.status_code in [Cte.UNAUTHORIZED, Cte.NOT_FOUND]:
            return r, False

        return r, tries < max_tries

    # %% User activity

    def get_user_activity(self, **args):

        count = args['count'] if 'count' in args else 10000
        since = args['since'] if 'since' in args else parser.parse('2006-01-01 00:00:00')
        until = args['until'] if 'until' in args else parser.parse('2022-01-01 00:00:00')

        if 'screen_name' in args:
            user = self.get_user(screen_name=args['screen_name'])
        elif 'user_id' in args:
            user = self.get_user(user_id=args['user_id'])
        elif 'user' in args:
            user = args['user']

        if user['protected']: return None

        count = min(count, user['statuses_count'])

        username, user_id_str = user['screen_name'], user['id_str']
        print(f'Scraping user activity of {username} {user_id_str}')

        tweet_collection = self.db[f'{TWEETS_U_C}_twint_{user_id_str}']

        n_tweets = tweet_collection.find({}).count()

        if self._cache and n_tweets > 0:
            tweet_list = list(tweet_collection.find({}))
            print(f'Getting user {user_id_str} activity from cache')
            print(f'{len(tweet_list)} events for user {user_id_str}')
            df = utools.create_df_from_tweet_list_twint(tweet_list)
            return df[df['timestamp'] > since.timestamp()]

        count_total = 0
        count_total_new = 0

        query = f"from:{username}"

        c = twint.Config()
        c.Search = query
        c.Hide_output = False
        c.User_full = False
        c.Limit = count
        c.Since = since.strftime('%Y-%m-%d %H:%M:%S')
        c.Until = until.strftime('%Y-%m-%d %H:%M:%S')
        c.Store_object = True
        twint.run.Search(c)

        tweets = twint.output.tweets_list

        for t in tweets:
            tweet = t.__dict__
            count_total += 1
            tmp = tweet_collection.find_one_and_update({'id_str': tweet['id_str']},
                                                       {"$set": tweet},
                                                       upsert=True)
            if tmp is None: count_total_new += 1

        print(f'{count_total} tweets retrieved\n\n')
        tweet_list = list(tweet_collection.find({}))

        print(f'{count_total_new} new events for user {user_id_str}')
        print(f'{len(tweet_list)} events for user {user_id_str}')

        df = utools.create_df_from_tweet_list_twint(tweet_list)
        return df[df['timestamp'] > since.timestamp()]




    # def get_user_activity(self, **args):
    #     raise NotImplementedError
    #     user_id = None
    #     call_args = {}
    #     user = self.get_user(**args)
    #
    #     max_results = args['max_results'] if 'max_results' in args else 100
    #     if 'user_id' in args:
    #         user_id = str(args['user_id'])
    #     elif 'screen_name' in args:
    #         user_id = str(user['id'])
    #         username = args['screen_name']
    #         print(f'Scraping user activity of {username} {user_id}')
    #
    #     total_items = user['tweets']
    #     n_items = min(args['n_items'], total_items) if 'n_items' in args else total_items
    #
    #     if 'until' in args: call_args['until'] = args['until']
    #
    #     tweet_collection = self.db[f'{TWEETS_U_C}{user_id}']
    #
    #     if self._cache and tweet_collection.count():
    #         tweet_list = list(tweet_collection.find({}))
    #         print(f'Getting user {user_id} activity from cache')
    #         print(f'{len(tweet_list)} events for user {user_id}')
    #         tweet_list = sorted(tweet_list, key=lambda k: k['datetime'])
    #         return tweet_list
    #
    #     count = 0
    #
    #     request = True
    #
    #     query = {'query': f'from:{user_id}', 'maxResults': max_results, 'fromDate': '201001010000'}
    #
    #     endpoint = 'tweets/search/'  # TODO
    #     while request:
    #
    #         r, success = self.try_request(endpoint, query)
    #         if not success: return None
    #
    #         for i, tweet in enumerate(r):
    #             tweet['datetime'] = parser.parse(tweet['created_at']).timestamp()
    #             if 'retweeted_status' in tweet:
    #                 tweet['retweeted_status']['datetime'] = parser.parse(
    #                     tweet['retweeted_status']['created_at']).timestamp()
    #
    #             if tweet_collection.find({'id_str': tweet['id_str']}).count() == 0.:
    #                 record_id = tweet_collection.insert_one(tweet).inserted_id
    #                 count += 1
    #
    #         if 'next' in r.json():
    #             query['next'] = r.json()['next']
    #         else:
    #             request = False
    #
    #     tweet_list = list(tweet_collection.find({}))
    #     print(f'{count} new events for user {user_id}')
    #     print(f'{len(tweet_list)} events for user {user_id}')
    #
    #     return tweet_list

    def get_user_activity_limited(self, **args):

        count = args['count'] if 'count' in args else 200
        max_num = args['max_num'] if 'max_num' in args else None
        min_cache_tweets = args['min_cache_tweets'] if 'min_cache_tweets' in args else 1
        if max_num: count = min(count, max_num)
        since = args['since'] if 'since' in args else datetime.datetime.strptime('2006-01-01', '%Y-%m-%d')

        user = None

        if 'user_id' in args:
            user = self.get_user(user_id=args['user_id'])
        elif 'screen_name' in args:
            user = self.get_user(screen_name=args['screen_name'])
        elif 'user' in args:
            user = args['user']

        if user['protected']: return None
        username, user_id = user['screen_name'], user['id']

        count = min(count, user['statuses_count'])
        print(f'Scraping user activity of {username} {user_id}')

        tweet_collection = self.db[f'{TWEETS_U_C}{user_id}']

        n_tweets = tweet_collection.find({}).count()

        if self._cache and n_tweets >= min_cache_tweets:
            tweet_list = list(tweet_collection.find({}))
            print(f'Getting user {user_id} activity from cache')
            print(f'{len(tweet_list)} events for user {user_id}')
            df = utools.create_df_from_tweet_list(tweet_list)
            return df[df['timestamp'] > since.timestamp()]

        count_total = 0
        count_total_new = 0

        request = True

        query = {'user_id': user_id, 'count': count}

        endpoint = 'statuses/user_timeline'
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

            print(f'{count} tweets retrieved\n\n')
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

    def update_user_activity(self, **args):

        count = args['count'] if 'count' in args else 200

        user = self.get_user(user_id=args['user_id']) if 'user_id' in args else self.get_user(
            screen_name=args['screen_name'])
        username, user_id = user['screen_name'], user['id']

        print(f'Scraping user activity of {username} {user_id}')

        coll_name = f'{TWEETS_U_C}{user_id}'

        if coll_name not in self.db.list_collection_names():
            print(f'ERROR: Scraper user {username} {user_id} activity first')
            return None

        tweet_collection = self.db[coll_name]

        tweets = list(tweet_collection.find({}))
        df = utools.create_df_from_tweet_list(tweets)
        since = df['datetime'].max()

        count_total = 0
        count_total_new = 0

        request = True

        query = {'user_id': user_id, 'count': count}

        endpoint = 'statuses/user_timeline'
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
                tweet_timestamp = parser.parse(tweet['created_at']).timestamp()
                if tweet_timestamp <= since.timestamp():
                    request = False
                    break

            print(f'{count} tweets retrieved\n\n')
            if count > 0:
                query['max_id'] = tweet['id'] - 1
            else:
                request = False

        tweet_list = list(tweet_collection.find({}))
        print(f'{count_total_new} new events for user {user_id}')
        print(f'{len(tweet_list)} events for user {user_id}')

        df = utools.create_df_from_tweet_list(tweet_list)
        return df[df['timestamp'] > since.timestamp()]

    # %% User Favorites

    def get_user_favorites(self, **args):

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

    def get_followers(self, **args):

        return self.get_connection(connection_type=Cte.FOLLOWERS, **args)

    def get_connection(self, connection_type, **args):

        user = None

        if 'screen_name' in args:
            user = self.get_user(screen_name=args['screen_name'])
        elif 'user_id' in args:
            user = self.get_user(user_id=args['user_id'])
        elif 'user' in args:
            user = args['user']

        max_num = args['max_num'] if 'max_num' in args else 1000000
        cache = args['cache'] if 'cache' in args else self._cache
        username, user_id = user['screen_name'], str(user['id'])

        print(f'Scraping {connection_type} of user {username} {user_id}')

        list_name = f'{connection_type}_list'

        data = self.db[list_name].find_one({"id_str": user['id_str']})

        data = data if data else {'id_str': user['id_str'], 'id_str_list': []}

        if cache and len(data['id_str_list']) > 0:
            ids_list = data['id_str_list']

            print(f'Getting user {user_id} {connection_type} from cache')
            print(f'{len(ids_list)} {connection_type} for user {user_id}')
            return ids_list

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

            self.db[list_name].find_one_and_update({"id_str": user['id_str']},
                                                   {"$set": data},
                                                   upsert=True)

        return data['id_str_list']  # list(users_collection.find({"id": {"$in": user[list_name]}}))

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


        if 'screen_name' in args:
            user_id_list = args['screen_name']

        elif 'user_id' in args:
            user_id_list = args['user_id']


        user_list = []

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

        return user_list


    def get_user(self, **args):
        '''
        This method returns a  user or list of users
        :param args:
        :return: user (json_format)
        '''

        query = {}
        find_query = {}
        if 'screen_name' in args:
            id_list = args['screen_name'] if isinstance(args['screen_name'], list) else [args['screen_name']]
            query = {'screen_name': ','.join(id_list)}
            find_query = {"screen_name": {"$in": id_list}}

        elif 'user_id' in args:
            id_list = args['user_id'] if isinstance(args['user_id'], list) else [args['user_id']]
            id_list = [str(i) for i in id_list]
            query = {'user_id': ','.join(id_list)}
            find_query = {"id_str": {"$in": id_list}}

        users_collection = self.db[USER_C]

        count_cache = users_collection.find(find_query).count()

        if self._cache and count_cache == len(id_list):
            user_list = list(users_collection.find(find_query))
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
                users_collection.find_one_and_update({'id_str': user['id_str']},
                                                     {"$set": user},
                                                     upsert=True)
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

    # %% Lists

    def get_list(self, list_id_str):

        lists_collection = self.db[LIST_C]

        list_ = lists_collection.find_one({'id_str': list_id_str})

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
                tmp = lists_collection.find_one_and_update({'id_str': l['id_str']},
                                                           {"$set": l},
                                                           upsert=True)
                count += 1
                total_count += 1

                if tmp is None: count_total_new += 1

            assert count == 1
            request = False
            print(f'{count} lists retrieved')

        print(f'{count_total_new} new List')

        return l

    def get_lists_of_user(self, list_type, **args):
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

        if 'screen_name' in args:
            user = self.get_user(screen_name=args['screen_name'])
        elif 'user_id' in args:
            user = self.get_user(user_id=args['user_id'])

        username, user_id = user['screen_name'], str(user['id'])
        print(f'Scraping Lists of {username} {user_id}')

        list_name = f'{LIST_C}_{list_type}'

        data = self.db[list_name].find_one({"id_str": user['id_str']})

        data = data if data else {'id_str': user['id_str'], 'lists': []}

        if not force and self._cache and len(data['lists']) > 0:
            print(f'Getting user {user_id} {list_name} from cache')
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
                    tmp = self.db[LIST_C].find_one_and_update({'id_str': l['id_str']},
                                                              {"$set": l},
                                                              upsert=True)
                    data['lists'].append(l['id_str'])
                    count += 1
                    total_count += 1

                    if tmp is None: count_total_new += 1

                    if total_count >= max_num:
                        request = False
                        break
            print(f'{count} lists retrieved')

            if 'next_cursor' in r.json() and r.json()['next_cursor'] != 0:
                query['cursor'] = r.json()['next_cursor']
            else:
                request = False

            data['lists'] = list(set(data['lists']))

            tmp = self.db[list_name].find_one_and_update({'id_str': user['id_str']},
                                                         {"$set": data},
                                                         upsert=True)

        print(f'{count_total_new} new Lists for user {user_id}')
        print(f"{len(data['lists'])} Lists for user {user_id}")

        return data['lists'][:max_num]

    def get_members_of_list(self, list_id_str, **args):

        # my_list = self.get_list(list_id_str=list_id_str)
        count = args['count'] if 'count' in args else 1000
        force = args['force'] if 'force' in args else False
        max_num = args['max_num'] if 'max_num' in args else 100000
        cache = args['cache'] if 'cache' in args else self._cache

        data = self.db[LIST_MEMBERS_C].find_one({'id_str': list_id_str})

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
                self.db[LIST_MEMBERS_C].find_one_and_update({'id_str': list_id_str},
                                                            {"$set": data},
                                                            upsert=True)
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

            self.db[LIST_MEMBERS_C].find_one_and_update({'id_str': list_id_str},
                                                        {"$set": data},
                                                        upsert=True)

        return data['member_list']

    def search_replies_to(self, **args):

        count = args['count'] if 'count' in args else 1000

        since = args['since'] if 'since' in args else parser.parse('2006-01-01 00:00:00')


        if 'screen_name' in args:
            user = self.get_user(screen_name=args['screen_name'])
        elif 'user_id' in args:
            user = self.get_user(user_id=args['user_id'])
        elif 'user' in args:
            user = args['user']

        screen_name, user_id_str = user['screen_name'], user['id_str']

        print(f'Scraping replies to {screen_name}')

        tweet_collection = self.db[f'{REPLYTO_C}{user_id_str}']

        from_user = None
        if 'from_screen_name' in args:
            from_user = self.get_user(screen_name=args['from_screen_name'])
        elif 'from_user_id' in args:
            from_user = self.get_user(user_id=args['from_user_id'])
        elif 'from_user' in args:
            from_user = args['from_user']

        if from_user:
            n_tweets = tweet_collection.count_documents({'user_id_str': from_user['id_str']})
        else:
            n_tweets = tweet_collection.count_documents({})

        if self._cache and n_tweets > 0:
            if from_user:
                tweet_list = list(tweet_collection.find({'user_id_str': from_user['id_str']}))
            else:
                tweet_list = list(tweet_collection.find({}))
            print(f'Getting replies to  {screen_name} from cache')
            print(f'{len(tweet_list)} events for user {screen_name}')

            df = utools.create_df_from_tweet_list_twint(tweet_list)
            return df[df['timestamp'] > since.timestamp()]

        count_total = 0
        count_total_new = 0

        query = f"to:{screen_name} from:{from_user['screen_name']}" if from_user else f'to:{screen_name}'

        c = twint.Config()
        c.Search = query
        c.Hide_output = False
        c.User_full = False
        c.Limit = count
        c.Since = since.strftime('%Y-%m-%d %H:%M:%S')
        c.Store_object = True
        twint.run.Search(c)

        tweets = twint.output.tweets_list

        for t in tweets:
            tweet = t.__dict__
            count_total += 1
            tmp = tweet_collection.find_one_and_update({'id_str': tweet['id_str']},
                                                       {"$set": tweet},
                                                       upsert=True)
            if tmp is None: count_total_new += 1

        print(f'{count_total} tweets retrieved\n\n')

        if from_user:
            tweet_list = list(tweet_collection.find({'user_id_str': from_user['id_str']}))
        else:
            tweet_list = list(tweet_collection.find({}))

        print(f'{count_total_new} new events for user {user_id_str}')
        print(f'{len(tweet_list)} events for user {user_id_str}')

        df = utools.create_df_from_tweet_list_twint(tweet_list)
        return df[df['timestamp'] > since.timestamp()]


    def search_tweets_with(self, query, **args):

        raise NotImplementedError
