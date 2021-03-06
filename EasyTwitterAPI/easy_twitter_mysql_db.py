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
import mysql.connector
import json


def create_table(name, cols_list):
    cols_str = ', '.join(cols_list)
    comm = f"CREATE TABLE {name} ({cols_str})"
    return comm


class EasyTwitterMySQLDB:

    def __init__(self, config_file):

        with open(config_file) as cred_data:
            config = json.load(cred_data)
        self.mydb = mysql.connector.connect(**config)

        self.cols_users = ['id_str', 'id', 'name', 'screen_name', 'location',
                           'description', 'url', 'protected', 'followers_count',
                           'friends_count', 'listed_count', 'created_at', 'favourites_count',
                           'geo_enabled', 'verified', 'statuses_count', 'profile_image_url'
                           ]
        self.cols_followees = ['user_id_str', 'fol_id_str']



    def create_database(self):
        cursor = self.mydb.cursor()

        cursor.execute("CREATE DATABASE twitter")
        cursor.close()


    def create_tables(self):
        self.create_users_table()
        self.create_tweets_table()
        self.create_favs_table()
        self.create_followees_table()
        self.create_lists_table()
        self.create_members_table()
        self.create_topics_table()
        self.create_topics_lists_table()


    def get_columns_table(self, table):
        if table == 'users':
            cols = self.cols_users.copy()
            # cols.append('last_modified')
            return cols
        elif table == 'lists':
            cols = self.cols_lists.copy()
            return cols
        elif table == 'followees':
            cols = self.cols_followees.copy()
            return cols
        elif table == 'tweets':
            cols = self.cols_tweets.copy()
            return cols
        elif table == 'favs':
            cols = self.cols_tweets.copy()
            return cols
        elif table == 'members':
            cols = self.cols_members.copy()
            return cols
        elif table == 'topics':
            cols = self.cols_topics.copy()
            return cols
        elif table == 'topics_lists':
            cols = self.cols_topics_lists.copy()
            df_topics = self.select('topics')
            cols.extend(list(df_topics['label']))
            return cols
        else:
            raise NotImplementedError
    def create_users_table(self):
        cursor = self.mydb.cursor()
        comm = create_table(name='users', cols_list=['id_str VARCHAR(64) PRIMARY KEY',
                                                     'id BIGINT UNSIGNED',
                                                     'name VARCHAR(128)',
                                                     'screen_name VARCHAR(128)',
                                                     'location VARCHAR(128)',
                                                     'description VARCHAR(512)',
                                                     'url VARCHAR(256)',
                                                     'protected VARCHAR(8)',
                                                     'followers_count INT UNSIGNED',
                                                     'friends_count INT UNSIGNED',
                                                     'listed_count INT UNSIGNED',
                                                     'created_at VARCHAR(128)',
                                                     'favourites_count  INT UNSIGNED',
                                                     'geo_enabled VARCHAR(8)',
                                                     'verified VARCHAR(8)',
                                                     'statuses_count INT UNSIGNED',
                                                     'profile_image_url VARCHAR(512)',
                                                     'datetime DATETIME DEFAULT CURRENT_TIMESTAMP',
                                                     ])
        cursor.execute(comm)
        cursor.close()

    @property
    def cols_tweets(self):
        return list(self._cols_tweets.keys())

    @property
    def _cols_tweets(self):
        return {'user_id_str': 'VARCHAR(64)',
                            'id_str': 'VARCHAR(64)',
                            'datetime': 'TIMESTAMP',
                            'retweet_count': 'INT UNSIGNED',
                            'favorite_count': 'INT UNSIGNED',
                            'in_reply_to_user_id_str': 'VARCHAR(64)',
                            'lang': 'VARCHAR(8)',
                            'text': 'VARCHAR(1024)',
                            'type': 'VARCHAR(16)',
                            'tweet_user_id_str': 'VARCHAR(64)',
                'emojis': 'TINYINT',
                'hashtags': 'TINYINT',
                'urls': 'TINYINT',
                'mentions': 'TINYINT',
                'text_processed': 'VARCHAR(1024)'}

    def create_tweets_table(self):
        cursor = self.mydb.cursor()
        cols_list = [f"{k} {v}" for k, v in self._cols_tweets.items()]
        cols_list.append('PRIMARY KEY (user_id_str, id_str)')
        cols_list.append('FOREIGN KEY (user_id_str) REFERENCES users(id_str)')

        comm = create_table(name='tweets', cols_list=cols_list)
        cursor.execute(comm)
        cursor.close()

    def create_favs_table(self):
        cursor = self.mydb.cursor()
        cols_list = [f"{k} {v}" for k, v in self._cols_tweets.items()]
        cols_list.append('PRIMARY KEY (user_id_str, id_str)')
        cols_list.append('FOREIGN KEY (user_id_str) REFERENCES users(id_str)')

        comm = create_table(name='favs', cols_list=cols_list)

        cursor.execute(comm)
        cursor.close()

    def create_followees_table(self):
        cursor = self.mydb.cursor()
        comm = create_table(name='followees', cols_list=['user_id_str VARCHAR(64)',
                                                         'fol_id_str VARCHAR(64)',
                                                         'PRIMARY KEY (user_id_str, fol_id_str)',
                                                         'FOREIGN KEY (user_id_str) REFERENCES users(id_str)'
                                                         ])
        cursor.execute(comm)
        cursor.close()

    @property
    def cols_lists(self):
        return list(self._cols_lists.keys())

    @property
    def _cols_lists(self):
        return {'user_id_str': 'VARCHAR(64)',
                            'id': 'BIGINT UNSIGNED',
                            'id_str': 'VARCHAR(64) PRIMARY KEY',
                            'name': 'VARCHAR(128)',
                            'uri': 'VARCHAR(256)',
                            'subscriber_count': 'INT UNSIGNED',
                            'member_count': 'INT UNSIGNED',
                            'mode': 'VARCHAR(64)',
                            'description': 'VARCHAR(512)',
                            'slug': 'VARCHAR(128)',
                            'full_name': 'VARCHAR(128)',
                            'created_at': 'DATETIME',
                            'user_screen_name': 'VARCHAR(64)',
                            'text_processed': 'VARCHAR(1024)'}
    def create_lists_table(self):
        cursor = self.mydb.cursor()

        cols_list = [f"{k} {v}" for k, v in self._cols_lists.items()]
        cols_list.append('FOREIGN KEY (user_id_str) REFERENCES users(id_str)')

        comm = create_table(name='lists', cols_list=cols_list)
        cursor.execute(comm)
        cursor.close()
    @property
    def cols_members(self):
        return list(self._cols_members.keys())

    @property
    def _cols_members(self):
        return {'list_id_str': 'VARCHAR(64)',
                'user_id_str': 'VARCHAR(64)'}
    def create_members_table(self):
        cursor = self.mydb.cursor()

        cols_list = [f"{k} {v}" for k, v in self._cols_members.items()]
        cols_list.append('PRIMARY KEY (list_id_str, user_id_str)')
        cols_list.append('FOREIGN KEY (list_id_str) REFERENCES lists(id_str)')
        cols_list.append('FOREIGN KEY (user_id_str) REFERENCES users(id_str)')

        comm = create_table(name='members', cols_list=cols_list)
        cursor.execute(comm)
        cursor.close()
    @property
    def cols_topics(self):
        return list(self._cols_topics.keys())

    @property
    def _cols_topics(self):
        return {'label': 'VARCHAR(64) PRIMARY KEY',
                'description': 'VARCHAR(1024)'}
    def create_topics_table(self):
        cursor = self.mydb.cursor()

        cols_list = [f"{k} {v}" for k, v in self._cols_topics.items()]
        comm = create_table(name='topics', cols_list=cols_list)
        cursor.execute(comm)
        cursor.close()

    @property
    def cols_topics_lists(self):
        return list(self._cols_topics_lists.keys())

    @property
    def _cols_topics_lists(self):
        return {'list_id_str':  'VARCHAR(64) PRIMARY KEY'}
    def create_topics_lists_table(self):
        cursor = self.mydb.cursor()

        cols_list = [f"{k} {v}" for k, v in self._cols_topics_lists.items()]
        cols_list.append('FOREIGN KEY (list_id_str) REFERENCES lists(id_str)')

        comm = create_table(name='topics_lists', cols_list=cols_list)
        cursor.execute(comm)
        cursor.close()

        df_t = self.select('topics')
        label_list = list(df_t['label'])
        for label in label_list:
            self.add_column('topics_lists', label, 'TINYINT DEFAULT -1')

    @property
    def cols_walls_collected(self):
        return list(self._cols_walls_collected.keys())

    @property
    def _cols_walls_collected(self):
        return {'user_id_str':  'VARCHAR(64) PRIMARY KEY',
                'is_collected':  'TINYINT'}
    def create_walls_collected_table(self):
        cursor = self.mydb.cursor()

        cols_list = [f"{k} {v}" for k, v in self._cols_walls_collected.items()]
        cols_list.append('FOREIGN KEY (user_id_str) REFERENCES users(id_str)')

        comm = create_table(name='walls_collected', cols_list=cols_list)
        cursor.execute(comm)
        cursor.close()
    def drop_table(self, table_name):
        drop_table_query = f"DROP TABLE {table_name}"
        cursor = self.mydb.cursor()
        cursor.execute(drop_table_query)

        cursor.close()

    def alter_table(self, table_name, alteration):
        query = f"ALTER TABLE {table_name} MODIFY COLUMN {alteration}"
        cursor = self.mydb.cursor()
        cursor.execute(query)

        cursor.close()
    def add_column(self, table_name, column_name, column_type):
        query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
        cursor = self.mydb.cursor()
        cursor.execute(query)

        cursor.close()



    def insert_update(self, table_name, cols_list, values):
        cursor = self.mydb.cursor()
        cols_str = ', '.join(cols_list)
        values_str = ', '.join([ str(v) for v in values])

        set_str = ', '.join([f"{c}=%s" for c in cols_list[1:]])


        comm = f"INSERT INTO {table_name} ({cols_str}) VALUES({values_str}) ON DUPLICATE KEY UPDATE {set_str}"

        cursor.execute(comm, tuple(values[1:]))
        self.mydb.commit()
        print(f'{table_name} ', cursor.rowcount, "record inserted/updated.")

        cursor.close()
    def insert(self, table, cols_list, val_list, ignore=False):
        cursor = self.mydb.cursor()
        cols_str = ','.join(cols_list)
        values_str = ', '.join(['%s' for i in range(len(cols_list))])
        if ignore:
            comm = f"INSERT IGNORE INTO {table} ({cols_str}) VALUES ({values_str})"
        else:
            comm = f"INSERT INTO {table} ({cols_str}) VALUES ({values_str})"
        if isinstance(val_list, list):
            cursor.executemany(comm, val_list)
        else:
            cursor.execute(comm, val_list)

        self.mydb.commit()
        print(f'{table} ', cursor.rowcount, "record inserted.")

        cursor.close()
        return

    def show_tables(self):
        cursor = self.mydb.cursor()
        comm = f"SHOW TABLES"
        cursor.execute(comm)

        results = cursor.fetchall()
        print('All existing tables:')
        for table in results:
            print('\t', table[0])
        cursor.close()
        return



    def get_filter_str(self, filter_id, filter_params):
        if filter_id == 'IN':
            values = filter_params['values']
            values_str = ', '.join(['%s', ] * len(values))
            return  f"{filter_params['col']} IN ({values_str})", values

        elif filter_id == 'EQ':
            value = filter_params['value']
            return  f"{filter_params['col']}=%s", [value]
        elif filter_id in ['>', '<', '>=', '<=', '<>']:
            value = filter_params['value']
            return  f"{filter_params['col']}{filter_id}%s", [value]

    def select(self, table, filter_dict=None, filter_concat=None, columns=None, only_one=False):
        cursor = self.mydb.cursor()
        columns = ', '.join(columns  if isinstance(columns, list) else self.get_columns_table(table))

        comm = f"SELECT {columns} FROM {table}"

        if isinstance(filter_dict, dict):
            if filter_concat is None: filter_concat = 'AND'
            filter_str_list = []
            filter_values_list = []
            for key, values in filter_dict.items():
                my_str, my_values = self.get_filter_str(filter_id=key, filter_params=values)
                filter_str_list.append(my_str)
                filter_values_list.extend(my_values)

            filter_str = f' {filter_concat} '.join(filter_str_list)
            filter_str = f"WHERE {filter_str}"

            comm = f"{comm} {filter_str}"
        print(comm)
        if isinstance(filter_dict, dict):
            cursor.execute(comm, tuple(filter_values_list))
        else:
            cursor.execute(comm)

        if not only_one:
            myresult = cursor.fetchall()

            out = pd.DataFrame.from_dict(myresult)
            if len(out)>0:
                cols =  self.get_columns_table(table)
                out.columns = cols
        else:
            out = cursor.fetchone()
            cols = self.get_columns_table(table)

            if out is not None: out = {key: value for key, value in zip(cols, out)}

        cursor.close()

        return out
