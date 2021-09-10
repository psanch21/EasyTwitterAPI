import os

import pandas as pd
from pymongo import MongoClient

from EasyTwitterAPI.utils.constants import Cte


class DBCte:
    LIST_C = 'list'
    LIST_MEMBERS_C = 'list_members'
    LIST_MEMSHIP_C = 'list_m'
    USER_FRIENDS_C = f'{Cte.FRIENDS}_list'
    USER_FOLL_C = f'{Cte.FOLLOWERS}_list'
    USER_C = 'user'
    TWEETS_U_C = 'tweets'
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

    def load(self, collection, filter_={}, find_one=False, return_as='cursor'):
        if collection not in self.collection_names():
            return None

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
        data =  self.load(collection=DBCte.USER_C, filter_=filter_, find_one=find_one, return_as=return_as)
        if return_as == 'df' and len(data) > 0:
            data.set_index(keys='id_str', inplace=True, drop=drop)
        return data

    def load_lists(self, filter_={}, find_one=False, return_as='cursor'):
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

    def load_favourites_user(self, user_id_str, filter_, find_one=False, return_as='cursor'):

        collection = f'{DBCte.FAVS_U_C}_{user_id_str[-1]}'

        assert 'id_str_timeline' not in filter_, print(f"The filter is {filter_}")
        filter_.update({'id_str_timeline': user_id_str})
        return self.load(collection=collection,
                         filter_=filter_,
                         find_one=find_one,
                         return_as=return_as)



    def create_indexes(self):
        raise NotImplementedError

    # %% UPDATE METHODS
    def update_data(self, collection, filter_, data):
        # assert collection in self.collection_names()

        if isinstance(data, dict) and '_id' in data: del data['_id']

        tmp = self.db[collection].find_one_and_update(filter_,
                                                      {"$set": data},
                                                      upsert=True)
        return tmp

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
