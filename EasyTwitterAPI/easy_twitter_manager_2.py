import json
import pickle
import numpy as np
import EasyTwitterAPI.utils.tools as utools
from datetime import timedelta


class EasyTwitterManager:

    def __init__(self, scraper, db, use_cache=True):
        self.scraper = scraper
        self.db = db
        self.use_cache = use_cache

    def activate_cache(self, value):
        self.use_cache = value

    def scrape_user(self, screen_name=None, id_str=None):
        if screen_name is not None:
            col = 'screen_name'
            id_value = screen_name
        else:
            col = 'id_str'
            id_value = id_str

        if self.use_cache:
            user = self.db.select('users',
                                  filter_dict={'EQ': {'col': col,
                                                      'value': id_value}
                                               },
                                  only_one=True)
            if user is not None:
                print(f'Getting user {id_value} from cache')
                return user

        user = self.scraper.get_user(**{col: id_value})
        if user is None:
            return user
        user = tuple([user[t] for t in self.db.cols_users])
        self.db.insert(table='users',
                       cols_list=self.db.cols_users,
                       val_list=user)
        user = self.db.select('users',
                              filter_dict={'EQ': {'col': col,
                                                  'value': id_value}
                                           },
                              only_one=True)
        return user

    def scrape_many_users(self, id_str_list):
        id_str_list_original = id_str_list.copy()
        if self.use_cache:
            df_user = self.db.select('users',
                                     filter_dict={'IN': {'col': 'id_str',
                                                         'values': id_str_list}
                                                  }
                                     )
            if df_user is not None and len(df_user) > 0:
                if len(df_user) == len(id_str_list):
                    return df_user
                id_str_list_cache = list(df_user['id_str'])
                id_str_list = utools.list_substract(id_str_list, id_str_list_cache)

        batches = int(np.ceil(len(id_str_list) / 100))
        user_list_all = []
        for i in range(batches):
            user_list = self.scraper.get_user(user_id=id_str_list[i*100:(i+1)*100])
            user_list = user_list if isinstance(user_list, list) else [user_list]
            user_list_all.extend(user_list)
        entries = []
        for user in user_list_all:
            user_t = tuple([user[t] for t in self.db.cols_users])

            entries.append(user_t)
        self.db.insert(table='users',
                       cols_list=self.db.cols_users,
                       val_list=entries,
                       ignore=True)
        df_user = self.db.select('users',
                                 filter_dict={'IN': {'col': 'id_str',
                                                     'values': id_str_list_original},
                                              })
        return df_user

    def scrape_user_activity(self, user, ignore=False, favs=False):
        table = 'favs' if favs else 'tweets'
        if self.use_cache:
            tweets = self.db.select(table,
                                    filter_dict={'EQ': {'col': 'user_id_str',
                                                        'value': user['id_str']}
                                                 }
                                    )
            if tweets is not None and len(tweets) > 0:
                print(f"Getting user activity {user['id_str']} from cache")
                return tweets

        if favs:
            df_tweets, users_in_tweets = self.scraper.get_user_favorites(user=user)
        else:
            df_tweets, users_in_tweets = self.scraper.get_user_activity_limited(user=user)

        entries = []
        for id_str_tmp, user_tmp in users_in_tweets.items():
            user_t = tuple([user_tmp[t] for t in self.db.cols_users])

            entries.append(user_t)
        self.db.insert(table='users',
                       cols_list=self.db.cols_users,
                       val_list=entries,
                       ignore=True)

        tweets_list = []

        for _, tweet in df_tweets.iterrows():
            t_dict = tweet.to_dict()
            t_dict['datetime'] = t_dict['datetime'].strftime('%Y-%m-%d %H:%M:%S')
            my_tweet = [user['id_str']]
            my_tweet.extend([t_dict[t] for t in self.db.cols_tweets[1:]])
            tweets_list.append(tuple(my_tweet))

        self.db.insert(table=table,
                       cols_list=self.db.cols_tweets,
                       val_list=tweets_list,
                       ignore=ignore)
        tweets = self.db.select(table,
                                filter_dict={'EQ': {'col': 'user_id_str',
                                                    'value': user['id_str']}
                                             }
                                )
        return tweets


    def scrape_user_followees(self, user):
        if self.use_cache:
            followees = self.db.select('followees',
                                       filter_dict={'EQ': {'col': 'user_id_str',
                                                           'value': user['id_str']}
                                                    })
            if followees is not None and len(followees) > 0:
                print(f"Getting user followees {user['id_str']} from cache")

                return list(followees['fol_id_str'])

        followees_list = self.scraper.get_followees(user=user)

        entries = [(user['id_str'], fol) for fol in followees_list]

        self.db.insert(table='followees',
                       cols_list=self.db.cols_followees,
                       val_list=entries)

        return followees_list

    def scrape_list(self, list_id_str):
        if self.use_cache:
            my_list = self.db.select('lists',
                                     filter_dict={'EQ': {'col': 'id_str',
                                                         'value': list_id_str}},
                                     only_one=True)
            if my_list is not None:
                print(f"Getting List {list_id_str} from cache")

                return my_list

        my_list, user = self.scraper.get_list(list_id_str=list_id_str)
        my_list['created_at'] = my_list['created_at'].strftime('%Y-%m-%d %H:%M:%S')

        user_cache = self.db.select('users',
                              filter_dict={'EQ': {'col': 'id_str',
                                                  'value':  user['id_str']}},
                              only_one=True)
        if user_cache is None:
            user = tuple([user[t] for t in self.db.cols_users])
            self.db.insert(table='users',
                           cols_list=self.db.cols_users,
                           val_list=user)

        my_list = tuple([my_list[t] for t in self.db.cols_lists])
        self.db.insert(table='lists',
                       cols_list=self.db.cols_lists,
                       val_list=my_list)
        my_list = self.db.select('lists',
                                 filter_dict={'EQ': {'col': 'id_str',
                                                     'value': list_id_str}},
                                 only_one=True)
        return my_list

    def scrape_list_members(self, list_id_str):
        if self.use_cache:
            members = self.db.select('members',
                                     filter_dict={'EQ': {'col': 'list_id_str',
                                                         'value': list_id_str}})
            if members is not None and len(members) > 0:
                print(f"Getting members {list_id_str} from cache")

                return list(members['user_id_str'])

        members = self.scraper.get_members_of_list(list_id_str)
        entries = []
        for user in members:
            user_t = tuple([user[t] for t in self.db.cols_users])

            entries.append(user_t)
        self.db.insert(table='users',
                       cols_list=self.db.cols_users,
                       val_list=entries,
                       ignore=True)
        entries = []
        for user in members:
            entries.append(tuple([list_id_str, user['id_str']]))
        self.db.insert(table='members',
                       cols_list=self.db.cols_members,
                       val_list=entries,
                       ignore=True)


        return [user['id_str'] for user  in members]


    def scrape_lists_of_user(self, user, list_type='m', max_num=100):
        assert list_type == 'm', 'Only mode implemented'
        if user['listed_count'] == 0:
            return []
        max_num = min(max_num, user['listed_count'])
        if self.use_cache:
            lists = self.db.select('members',
                                     filter_dict={'EQ': {'col': 'user_id_str',
                                                         'value': user['id_str']}})

            if lists is not None and len(lists) >=max_num:
                print(f"Getting lists of  {user['id_str']} from cache")

                return list(lists['list_id_str'])

        lists, creators = self.scraper.get_lists_of_user(list_type=list_type,
                                                         user=user,
                                                         max_num=max_num)


        # Add creators of the Lists
        entries = []
        for cre in creators:
            user_t = tuple([cre[t] for t in self.db.cols_users])

            entries.append(user_t)
        self.db.insert(table='users',
                       cols_list=self.db.cols_users,
                       val_list=entries,
                       ignore=True)
        # Add Lists
        entries = []
        for l in lists:
            l = tuple([l[t] for t in self.db.cols_lists])
            entries.append(l)

        self.db.insert(table='lists',
                       cols_list=self.db.cols_lists,
                       val_list=entries,
                       ignore=True)

        # Add Lists of user
        entries = []
        for l in lists:
            entries.append(tuple([l['id_str'], user['id_str']]))
        self.db.insert(table='members',
                       cols_list=self.db.cols_members,
                       val_list=entries,
                       ignore=True)

        return [l['id_str'] for l in lists]

    def scrape_wall(self, user_id_str):
        seeker = self.scrape_user(id_str=user_id_str)
        tweets = self.scrape_user_activity(seeker)
        tweets = self.scrape_user_activity(seeker, favs=True)

        favs = self.scrape_user_activity(seeker, favs=True)
        followees = self.scrape_user_followees(seeker)
        # %%
        df_foll = self.scrape_many_users(id_str_list=followees)
        # %%

        for _, fol in df_foll.iterrows():
            if fol['protected'] == '1': continue
            friends_fol = self.scrape_user_followees(fol)
            if fol['statuses_count'] > 0:
                tweets = self.scrape_user_activity(fol)


    def get_user_activity(self, user_id_str, init_dt, end_dt, favs=False):
        table = 'favs' if favs else 'tweets'

        init_dt =init_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_dt =end_dt.strftime("%Y-%m-%d %H:%M:%S")
        df_activity = self.db.select(table,
                                 filter_dict={'EQ': {'col': 'user_id_str',
                                                     'value': user_id_str},
                                              '>=': {'col': 'datetime',
                                                     'value': init_dt},
                                              '<=': {'col': 'datetime',
                                                     'value': end_dt}
                                              })
        return df_activity

    def get_wall_from_db(self, user_id_str, init_dt, end_dt):
        user = self.scrape_user(id_str=user_id_str)
        user_list = self.scrape_user_followees(user)

        init_dt =init_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_dt =end_dt.strftime("%Y-%m-%d %H:%M:%S")

        df_wall = self.db.select('tweets',
                                 filter_dict={'IN': {'col': 'user_id_str',
                                                     'values': user_list},
                                              '>=': {'col': 'datetime',
                                                     'value': init_dt},
                                              '<=': {'col': 'datetime',
                                                     'value': end_dt}
                                              })

        df_wall = df_wall[~df_wall.tweet_user_id_str.eq(user_id_str)]
        return df_wall[~df_wall.type.eq('Answer')]


    def save_wall_G(self, list_id_str, T=30, L=10, Q=0, sigma=8):
        '''

        :param list_id_str:
        :param T: Number of days to look back from the list creation time
        :param L:  Minimum number of times listed to be considered an expert
        :param Q: Minimum number of times listed in Lists associated with the topic of the List
        :param sigma: Bandwitdth of the kernel in minutes

        :return:
        '''

        my_list = self.scrape_list(list_id_str=list_id_str)
        end_dt = my_list['created_at']
        init_dt = end_dt - timedelta(days=T)
        self.scrape_wall(user_id_str=my_list['user_id_str'])
        df_wall = self.get_wall_from_db(my_list['user_id_str'],
                                           init_dt=init_dt,
                                           end_dt=end_dt
                                           )

        print(f"\nMin datetime: {df_wall['datetime'].min()}")
        print(f"Max datetime: {df_wall['datetime'].max()}\n")
        df_t_seeker = self.get_user_activity(my_list['user_id_str'], init_dt, end_dt)
        df_l_seeker = self.get_user_activity(my_list['user_id_str'], init_dt, end_dt, favs=True)
        num_posts_seeker = len(df_t_seeker) + len(df_l_seeker)

        # Get times at which the sxeeker is active in minutes  timestamp in minutes
        seeker_dt_list = []

        seeker_dt_list.extend(list(df_t_seeker['datetime'].map(lambda x: x.timestamp())/60))
        seeker_dt_list.extend(list(df_l_seeker['datetime'].map(lambda x: x.timestamp())/60))
        seeker_dt = np.array(seeker_dt_list)

        # Get type of user for all users in the wall
        users_in_wall= [my_list['user_id_str']]
        users_in_wall.extend(list(df_wall['tweet_user_id_str'].unique()))
        users_in_wall = list(set(users_in_wall))
        df_users = self.scrape_many_users(id_str_list=users_in_wall)
        print(f' Number of unique users within the wall: {len(df_users)}')
        df_users['type'] = 'User'
        seeker = df_users[df_users.id_str.eq(my_list['user_id_str'])]
        assert len(seeker) == 1
        df_users.loc[seeker.index, 'type'] = 'Seeker'
        df_users.loc[seeker.index, 'rate_total'] = num_posts_seeker/T


        #
        if Q > 0:
            for _, user in df_users.iterrows():
                if user['listed_count'] < L: continue
                lists = self.scrape_lists_of_user(user)
        else:
            df_users.loc[df_users.listed_count >= L, 'type'] = 'Expert'

        members = self.scrape_list_members(list_id_str)

        df_users.loc[df_users.id_str.isin(members), 'type'] = 'Member'

        # Compute rate of tweets in the given interval.
        # For each user, How many tweets per day appear in the wall?

        for user_id_str, df_u in df_wall.groupby('tweet_user_id_str'):
            rate = len(df_u) / T  # Tweets/day
            df_users.loc[df_users.id_str.eq(user_id_str), 'rate_total'] = len(df_u) / T

        interactions_dict = {}

        for id_str_i in users_in_wall:
            for id_str_j in users_in_wall:
                if id_str_i == id_str_j: continue
                id_ = (id_str_i, id_str_j)
                if id_str_i == my_list['user_id_str']:
                    dt_i = seeker_dt
                else:
                    df_i = df_wall[df_wall.tweet_user_id_str.eq(id_str_i)]
                    dt_i = list(df_i['datetime'].map(lambda x: x.timestamp())/60)

                if id_str_j == my_list['user_id_str']:
                    dt_j = seeker_dt
                else:
                    df_j = df_wall[df_wall.tweet_user_id_str.eq(id_str_j)]
                    dt_j = list(df_j['datetime'].map(lambda x: x.timestamp())/60)

                diff_dt = dt_i[:,np.newaxis] - dt_j
                avg_vis_tweets = np.sum(np.exp(-diff_dt**2/sigma))

                if avg_vis_tweets >=1:
                    if id_ not in interactions_dict: interactions_dict[id_] = {}
                    interactions_dict[id_]['avg_vis_tweets'] = avg_vis_tweets


)

        return

