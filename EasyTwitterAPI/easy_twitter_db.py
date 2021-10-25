import os

import pandas as pd
import pymongo
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from EasyTwitterAPI.utils.constants import Cte


class DBCte:
    LIST_C = 'list'
    LIST_MEMBERS_C = 'list_members'
    LIST_MEMSHIP_C = 'list_m'
    USER_FRIENDS_C = f'{Cte.FRIENDS}_list'
    USER_FOLL_C = f'{Cte.FOLLOWERS}_list'
    USER_C = 'user'
    TWEETS_U_C = 'tweets'
    STATUSES = 'statuses'
    REPLYTO_C = 'replies_'

    TWEETS_V2 = 'tweets_v2'
    FAVS_U_C = 'favs'
    RELAT_C = 'relationship'


class EasyTwitterDB:

    def __init__(self, db_name, host, tlsAllowInvalidCertificates=True):

        if host == 'localhost':
            self.db = MongoClient('localhost', 27017, tlsAllowInvalidCertificates=tlsAllowInvalidCertificates)[db_name]
        else:
            # For example MongoDB dataset
            self.db = MongoClient(host, tlsAllowInvalidCertificates=tlsAllowInvalidCertificates)[db_name]

        self.db_name = db_name
        self.host = host

        self.coll_names = None
        self.coll_tweets_names = None
        self.coll_favs_names = None

    def backup(self, root_dir, collection_list=None):
        root_dir = os.path.join(root_dir, self.db_name)
        if collection_list is None:
            collection_list = self.collection_names()

        for coll in collection_list:
            out_file = os.path.join(root_dir, f'{coll}.json')
            os.system(f"mongoexport --db {self.db_name} -c {coll} --out {out_file}")
        return

    def collection_names(self):
        return self.db.list_collection_names()

    def refresh_collection_names(self):
        self.coll_names = self.collection_names()
        self.coll_tweets_names = [n for n in self.coll_names if 'tweets_' in n]
        self.coll_favs_names = [n for n in self.coll_names if 'favs_' in n]

    def load(self, collection, filter_=None, find_one=False, return_as='cursor'):
        if collection not in self.collection_names():
            if return_as == 'df':
                return pd.DataFrame()
            elif return_as == 'list':
                return []
            else:
                return None

        if filter_ is None: filter_ = {}
        if find_one:
            data = self.db[collection].find_one(filter_)
            if isinstance(data, dict) and '_id' in data: del data['_id']
            return data
        else:
            out = self.db[collection].find(filter_)

            if return_as == 'df':
                df = pd.DataFrame.from_dict(out)
                if df is None:
                    df = pd.DataFrame()
                return df
            elif return_as == 'list':
                return list(out)
            else:
                return out

    def load_users(self, filter_, find_one=False, return_as='cursor', drop=True):
        data = self.load(collection=DBCte.USER_C, filter_=filter_, find_one=find_one, return_as=return_as)
        if return_as == 'df' and len(data) > 0:
            data.set_index(keys='id_str', inplace=True, drop=drop)
        return data

    def load_lists(self, filter_={}, find_one=False, return_as='cursor'):
        # TODO:
        return self.load(collection=DBCte.LIST_C, filter_=filter_, find_one=find_one, return_as=return_as)

    def load_members_of_lists(self, filter_={}, find_one=False, return_as='cursor'):
        return self.load(collection=DBCte.LIST_MEMBERS_C, filter_=filter_, find_one=find_one, return_as=return_as)

    def load_lists_of_user(self, list_type, filter_={}, find_one=False, return_as='cursor'):
        collection = f'{DBCte.LIST_C}_{list_type}'

        return self.load(collection=collection, filter_=filter_, find_one=find_one, return_as=return_as)

    def load_statuses_user(self, user_id_str, filter_, find_one=False, return_as='cursor'):

        collection = f'{DBCte.TWEETS_U_C}_{user_id_str[-1]}'

        assert 'id_str_timeline' not in filter_, print(f"The filter is {filter_}")
        filter_.update({'id_str_timeline': user_id_str})
        return self.load(collection=collection,
                         filter_=filter_,
                         find_one=find_one,
                         return_as=return_as)

    def load_statuses(self, tweet_id_str_list, filter_, find_one=False, return_as='cursor'):

        collection = DBCte.STATUSES

        filter_.update({'id_str': {'$in': tweet_id_str_list}})
        return self.load(collection=collection,
                         filter_=filter_,
                         find_one=find_one,
                         return_as=return_as)

    def load_activity(self, id_str_user_list, min_dt, max_dt, return_as='df'):
        df_list = []
        id_str_user_list = id_str_user_list[:10]
        for i in range(10):
            id_str_i = [id_str for id_str in id_str_user_list if id_str[-1] == str(i)]
            collection = f'{DBCte.TWEETS_U_C}_{i}'
            filter_ = {'id_str_timeline': {'$in': id_str_i},
                       'datetime': {'$gt': min_dt, '$lt': max_dt}}
            df_activity = self.load(collection=collection,
                                    filter_=filter_,
                                    find_one=False,
                                    return_as=return_as)
            if len(df_activity) > 0:
                df_list.append(df_activity)

        if len(df_list) > 0:
            return pd.concat(df_list)
        else:
            return pd.DataFrame()

    def load_favourites_user(self, user_id_str, filter_, find_one=False, return_as='cursor'):

        collection = f'{DBCte.FAVS_U_C}_{user_id_str[-1]}'

        assert 'id_str_timeline' not in filter_, print(f"The filter is {filter_}")
        filter_.update({'id_str_timeline': user_id_str})
        return self.load(collection=collection,
                         filter_=filter_,
                         find_one=find_one,
                         return_as=return_as)

    def create_indexes(self):
        for i in range(10):
            collection = f'{DBCte.TWEETS_U_C}_{i}'
            print(f"Creating indexes for collection {collection}")
            self.db[collection].list_indexes()
            self.db[collection].create_index("id_str_timeline")
            self.db[collection].create_index(keys=[("id_str", pymongo.ASCENDING),
                                                   ("id_str_timeline", pymongo.ASCENDING)],
                                             unique=True)

        for i in range(10):
            collection = f'{DBCte.FAVS_U_C}_{i}'
            print(f"Creating indexes for collection {collection}")
            self.db[collection].create_index("id_str_timeline")
            self.db[collection].create_index(keys=[("id_str", pymongo.ASCENDING),
                                                   ("id_str_timeline", pymongo.ASCENDING)],
                                             unique=True)

        print(f"Creating indexes for collection {DBCte.LIST_C}")
        try:
            self.db[DBCte.LIST_C].create_index("id_str", unique=True)
        except DuplicateKeyError:
            print('Duplicated key! Removing duplicates (run again to create index)')

            df = self.load_lists({}, return_as='df')
            df_g = df.groupby('id_str').count()
            cond = df_g['created_at'] > 1
            id_str_list = list(df_g[cond].index)
            out = self.db[DBCte.LIST_C].delete_many({'id_str': {'$in': id_str_list}})
            print(out)

        print(f"Creating indexes for collection {DBCte.USER_C}")
        try:
            self.db[DBCte.USER_C].create_index("id_str", unique=True)
        except DuplicateKeyError:
            print('Duplicated key! Removing duplicates (run again to create index)')

            df = self.load_users({}, return_as='df')
            df_g = df.groupby('id_str').count()
            cond = df_g['created_at'] > 1
            id_str_list = list(df_g[cond].index)
            out = self.db[DBCte.USER_C].delete_many({'id_str': {'$in': id_str_list}})
            print(out)
        print(f"Creating indexes for collection {DBCte.STATUSES}")
        try:
            self.db[DBCte.STATUSES].create_index("id_str", unique=True)
        except DuplicateKeyError:
            print('Duplicated key! WTF happened')


    # %% UPDATE METHODS
    def update_data(self, collection, filter_, data):
        # assert collection in self.collection_names()

        if isinstance(data, dict) and '_id' in data: del data['_id']
        tmp = self.db[collection].find_one_and_update(filter_,
                                                      {"$set": data},
                                                      upsert=True)
        return tmp

    def insert_many(self, collection, data_list):
        # assert collection in self.collection_names()

        for data in data_list:
            if isinstance(data, dict) and '_id' in data: del data['_id']
        return self.db[collection].insert_many(data_list)

    def update_lists_of_user(self, list_type, filter_, data):
        collection = f'{DBCte.LIST_C}_{list_type}'
        self.update_data(collection=collection, filter_=filter_, data=data)

    def update_user(self, filter_, data):
        return self.update_data(collection=DBCte.USER_C, filter_=filter_, data=data)

    def update_list(self, filter_, data):
        return self.update_data(collection=DBCte.LIST_C, filter_=filter_, data=data)

    def update_members_of_list(self, filter_, data):
        return self.update_data(collection=DBCte.LIST_MEMBERS_C, filter_=filter_, data=data)

    def update_statuses_user(self, filter_, data):
        user_id_str = data['id_str_timeline']
        collection = f'{DBCte.TWEETS_U_C}_{user_id_str[-1]}'
        filter_['id_str_timeline'] = data['id_str_timeline']
        return self.update_data(collection=collection, filter_=filter_, data=data)
    def update_statuses(self, filter_, data):
        collection = DBCte.STATUSES
        return self.update_data(collection=collection, filter_=filter_, data=data)

    def insert_many_statuses(self, data_list):
        return self.insert_many(DBCte.STATUSES, data_list)

    def insert_many_statuses_user(self, data_list, id_str_user):
        return self._insert_many_activity(DBCte.TWEETS_U_C, data_list, id_str_user)

    def _insert_many_activity(self, activity_type, data_list, id_str_user):
        if len(data_list) == 0: return
        user_id_str = id_str_user
        collection = f'{activity_type}_{user_id_str[-1]}'
        return self.insert_many(collection=collection, data_list=data_list)

    def insert_many_favourites(self, data_list, id_str_user):
        return self._insert_many_activity(DBCte.FAVS_U_C, data_list, id_str_user)

    def update_favourites_user(self, filter_, data):
        user_id_str = data['id_str_timeline']
        collection = f'{DBCte.FAVS_U_C}_{user_id_str[-1]}'
        filter_['id_str_timeline'] = data['id_str_timeline']
        return self.update_data(collection=collection, filter_=filter_, data=data)

    def count(self, collection='activity', filter={}):
        if collection == 'user':
            collection = self.db[DBCte.USER_C]
        elif collection == Cte.FOLLOWERS:
            collection = self.db[DBCte.USER_FOLL_C]
        elif collection == Cte.FRIENDS:
            collection = self.db[DBCte.USER_FRIENDS_C]
        else:
            raise NotImplementedError

        return collection.find(filter).count()

    def info_db(self, full=False):
        raise NotImplementedError
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
