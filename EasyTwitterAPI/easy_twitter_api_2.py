import datetime
import json
import os
import time
from datetime import timedelta

import numpy as np
import pandas as pd
import twint
from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterError
from dateutil import parser

import EasyTwitterAPI.utils.tools as utools
from EasyTwitterAPI.utils.constants import Cte


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

    def __init__(self, cred_file,  sleep_secs=330):
        consumer_key, consumer_secret, access_token, access_token_secret = get_credentials(cred_file)


        self.alternative_cred_file_list = []
        self.pos_cred = 0

        self.api = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)
        self.api_premium = None
        self.premium_endpoint = None

        self.coll_names = None
        self.coll_tweets_names = None
        self.coll_favs_names = None

        self.sleep_secs = sleep_secs



    def set_cred_premium(self, cred_file_premium, dev_label='awareness'):
        consumer_key, consumer_secret, access_token, access_token_secret = get_credentials(cred_file_premium)
        self.api_premium = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)
        self.premium_endpoint = f'tweets/search/fullarchive/:{dev_label}'


    def set_alternative_cred_file_list(self, file_list):
        self.alternative_cred_file_list = file_list
        self.pos_cred = np.random.randint(len(file_list))



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

        # TODO

        assert self.premium_endpoint is not None

        count = args['count'] if 'count' in args else 500
        max_num = args['max_num'] if 'max_num' in args else None
        min_cache_tweets = args['min_cache_tweets'] if 'min_cache_tweets' in args else 1
        if max_num: count = min(count, max_num)
        toDate = args['toDate'] if 'toDate' in args else None
        fromDate = args['fromDate'] if 'fromDate' in args else None

        toDate_str = args['toDate'].strftime("%Y%m%d%H%m") if 'toDate' in args else None
        fromDate_str = args['fromDate'].strftime("%Y%m%d%H%m") if 'fromDate' in args else None

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

        print(f'Scraping {self.premium_endpoint} of {username} {user_id}')


        count_total = 0

        tweets_list = []

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


    def _get_user_activity_type(self, endpoint, user, **args):

        user_id = user['id_str']
        count = args['count'] if 'count' in args else 200
        max_num = args['max_num'] if 'max_num' in args else None
        tmp = 'statuses_count' if 'statuses' in endpoint else 'favourites_count'

        if max_num:
            count = min(count, max_num)
        else:
            max_num = min(3600, user[tmp])
        if 'since' in args:
            if isinstance(args['since'], str):
                since = datetime.datetime.strptime(args['since'], '%Y-%m-%d')
            else:
                since = args['since']
        else:
            since = datetime.datetime.strptime('2006-01-01', '%Y-%m-%d')

        until = args['until'] if 'until' in args else datetime.datetime.now()



        print(f'Scraping {endpoint} of {user_id}')


        since_id = args['since_id'] if 'since_id' in args else None

        count_total = 0

        request = True

        query = {'user_id': user_id, 'count': count, 'tweet_mode': 'extended'}
        if since_id: query['since_id'] = since_id

        min_timestamp = since.timestamp()

        tweets_list = []
        users_in_tweets = {}
        patient = 0
        while request:

            r, success = self.try_request(endpoint, query)
            if not success: return None

            count = 0

            for i, tweet in enumerate(r):
                count += 1
                count_total += 1
                # tweets_list.append(tweet)
                if 'retweeted_status' in tweet:
                    r_status = tweet['retweeted_status']
                    if 'user' in r_status:
                        users_in_tweets[r_status['user']['id_str']] = r_status['user']

                tweet_clean = utools.clean_tweet(tweet)
                tweets_list.append(utools.clean_tweet(tweet))

                if max_num and count_total >= max_num:
                    request = False
                    break

            print(f'{count} tweets retrieved\n\n')
            if count > 0:
                patient = 0
                tweet_timestamp = parser.parse(tweet['created_at']).timestamp()
                if tweet_timestamp < min_timestamp:
                    request = False
                else:
                    query['max_id'] = tweet['id'] - 1
            else:
                if patient ==3:
                    request = False
                else:
                    patient +=1


        print(f"{len(tweets_list)} events for user {user_id} {user[tmp]} ")

        df = pd.DataFrame.from_dict(tweets_list)
        if len(df) > 0:
            df = df[df['timestamp'] >= since.timestamp()]
            df = df[df['timestamp'] <= until.timestamp()]
        print(f'{len(tweets_list)} events for user {user_id} in [{since}, {until}]')
        return df, users_in_tweets

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

    # %% Connections

    def get_followees(self, **args):

        return self.get_connection(connection_type=Cte.FRIENDS, **args)

    def get_followers(self, **args):

        return self.get_connection(connection_type=Cte.FOLLOWERS, **args)

    def get_connection(self, connection_type, user, **args):



        max_num = args['max_num'] if 'max_num' in args else 100000
        str_format = args['str_format'] if 'str_format' in args else True
        username, user_id = user['screen_name'], user['id']

        print(f'Scraping {connection_type} of user {username} {user_id}')


        request = True
        endpoint = f'{connection_type}/ids'
        query = {'user_id': user_id, 'count': 5000}
        total_count = 0
        connections_list = []
        while request:

            r, success = self.try_request(endpoint, query)
            if not success: return []

            count = 0
            for i, u_id in enumerate(r):
                connections_list.append(u_id)
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

            connections_list = list(set(connections_list))


        output = [str(i) for i in connections_list] if str_format else connections_list
        return output

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

        user_list = list(self.load_cache_data(collection=USER_C, filter_=filter_))
        if len(user_list) == len(user_id_list_total):
            return utools.create_df_from_user_list(user_list, drop=drop)

        user_list = []
        df_users = utools.create_df_from_user_list(user_list, drop=drop)

        if 'screen_name' in args:
            user_id_list = list(set(user_id_list_total) - set(df_users['screen_name'].unique()))

        elif 'user_id' in args:
            user_id_list = list(set(user_id_list_total) - set(df_users['id_str'].unique()))

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
        user_list = list(self.load_cache_data(collection=USER_C, filter_=filter_))
        return utools.create_df_from_user_list(user_list, drop=drop)

    def get_user(self, **args):
        '''
        This method returns a  user or list of users
        :param args:
        :return: user (json_format)
        '''

        query = {}
        if 'screen_name' in args:
            id_list = args['screen_name'] if isinstance(args['screen_name'], list) else [args['screen_name']]
            query = {'screen_name': ','.join(id_list)}

        elif 'user_id' in args:
            id_list = args['user_id'] if isinstance(args['user_id'], list) else [args['user_id']]
            id_list = [str(i) for i in id_list]
            query = {'user_id': ','.join(id_list)}
        else:
            raise NotImplementedError


        request = True
        endpoint = 'users/lookup'
        user_list = []
        while request:

            r, success = self.try_request(endpoint, query)
            if not success: return None

            count = 0
            for i, user in enumerate(r):
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


        endpoint = 'lists/show'

        request = True

        query = {'list_id': list_id_str}
        total_count = 0
        while request:

            r, success = self.try_request(endpoint, query)
            if not success: return None, None

            count = 0
            for i, l in enumerate(r):
                l, user  = utools.clean_list(l)
                count += 1
                total_count += 1

            assert count == 1
            request = False

        print(f'{total_count} List retrieved')

        return l, user

    def refractor_list_collection(self):
        lists = self.db[LIST_C].find({})
        lists_db = pd.DataFrame.from_dict(lists)
        n_lists = len(lists_db)

        i = 0
        lists_to_be_updated = []
        for _, list_ in lists_db.iterrows():
            i += 1
            if i % 100 == 0: print(f'Processed lists {i}/{n_lists}')
            if 'user_id_str' not in list_ or not isinstance(list_['user_id_str'], str):
                lists_to_be_updated.append(utools.clean_list(list_))

        n_lists = len(lists_to_be_updated)
        for i, list_ in enumerate(lists_to_be_updated):
            if i % 10 == 0: print(f'Updated lists {i}/{n_lists}')
            self.find_and_update_list(list_.to_dict())
        return

    def get_lists_of_user_full(self, list_type, **args):
        l_id_str_list = self.get_lists_of_user(list_type, **args)
        if l_id_str_list is None: l_id_str_list = []
        cursor = self.load_cache_data(collection=LIST_C, filter_={'id_str': {'$in': l_id_str_list}})
        df_lists = utools.create_df_list(cursor)
        if len(df_lists) == len(l_id_str_list): return df_lists
        l_remaining = set(l_id_str_list) - set(df_lists['id_str'].unique())
        for id_str in list(l_remaining):
            l = self.get_list(list_id_str=id_str)

        cursor = self.load_cache_data(collection=LIST_C, filter_={'id_str': {'$in': l_id_str_list}})
        df_lists = utools.create_df_list(cursor)
        assert len(df_lists) == len(l_id_str_list)

        return df_lists

    def get_lists_of_user(self, list_type, user, **args):
        '''
        https://developer.twitter.com/en/docs/accounts-and-users/create-manage-lists/api-reference/get-lists-memberships

        This method returns a list of Twitter Lists
        :param list_type: own (o), membership (m) or  subsriptions (s)
        :param args:
        :return:
        '''


        count = args['count'] if 'count' in args else 1000
        max_num = args['max_num'] if 'max_num' in args else 100000
        force = args['force'] if 'force' in args else False
        since = args['since'] if 'since' in args else datetime.datetime.strptime('2009-01-01', '%Y-%m-%d')


        username, user_id = user['screen_name'], str(user['id'])
        print(f'Scraping Lists of {username} {user_id}')


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
        total_count = 0

        lists_list =[]
        creators_list = []

        while request:

            r, success = self.try_request(endpoint, query)
            if not success: return None

            count = 0
            for i, json in enumerate(r):
                for l in json['lists']:
                    l, creator = utools.clean_list(l)
                    lists_list.append(l)
                    creators_list.append(creator)

                    count += 1
                    total_count += 1
                    if l['created_at']< since:
                        request = False
                        break

                    if total_count >= max_num:
                        request = False
                        break
            print(f'{count} lists retrieved')

            if 'next_cursor' in r.json() and r.json()['next_cursor'] != 0:
                query['cursor'] = r.json()['next_cursor']
            else:
                request = False



        print(f"{len(lists_list)} Lists for user {user_id}")

        return lists_list[:max_num], creators_list[:max_num]

    def update_lists_of_user(self, list_type, min_dt, **args):
        cache = self._cache
        self.activate_cache(True)
        df = self.get_lists_of_user_full(list_type, **args)
        if df is None or len(df) == 0: return df
        if df['datetime'].max() < min_dt:
            self.activate_cache(False)
            _ = self.get_lists_of_user(list_type=list_type, since=df['datetime'].max(), **args)
            self.activate_cache(cache)

        full = args['full'] if 'full' in args else False
        if full:
            df = self.get_lists_of_user_full(list_type, **args)
        else:
            df = self.get_lists_of_user(list_type, **args)
        return df


    def get_members_of_list(self, list_id_str, **args):

        count = args['count'] if 'count' in args else 1000
        force = args['force'] if 'force' in args else False
        max_num = args['max_num'] if 'max_num' in args else 100000



        request = True
        endpoint = f'lists/members'
        query = {'list_id': list_id_str, 'count': count}

        total_count = 0

        members_list = []

        while request:

            r, success = self.try_request(endpoint, query)
            if not success:
                members_list = r.status_code

                return members_list

            count = 0
            for i, u in enumerate(r):
                members_list.append(u)
                # members_list.append(u['id'])

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

            # members_list = list(set(members_list))


        return members_list

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

    def search_replies_to_2(self, **args):
        '''
        This method users the Twitter API
        :param args:
        :return:
        '''

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
            tweet_cleaned = utools.clean_tweet(tweet)

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
            tweet_cleaned = utools.clean_tweet(tweet)

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
