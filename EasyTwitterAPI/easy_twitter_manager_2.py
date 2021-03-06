import os
from datetime import timedelta

import neattext as nt
import networkx as nx
import numpy as np
import pandas as pd

import EasyTwitterAPI.utils.tools as utools


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
            user_list = self.scraper.get_user(user_id=id_str_list[i * 100:(i + 1) * 100])
            user_list = user_list if isinstance(user_list, list) else [user_list]
            user_list_all.extend(user_list)
        entries = []
        for user in user_list_all:
            if user is None: continue
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

    def scrape_user_activity(self, user, ignore=False, favs=False, since='2020-08-01'):
        assert user is not None
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
            df_tweets, users_in_tweets = self.scraper.get_user_favorites(user=user, since=since)
        else:
            df_tweets, users_in_tweets = self.scraper.get_user_activity_limited(user=user, since=since)

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

    def scrape_user_followees(self, user, ignore=False, max_num=5000):
        if self.use_cache:
            followees = self.db.select('followees',
                                       filter_dict={'EQ': {'col': 'user_id_str',
                                                           'value': user['id_str']}
                                                    })
            if followees is not None and len(followees) > 0:
                print(f"Getting user followees {user['id_str']} from cache")

                return list(followees['fol_id_str'])

        followees_list = self.scraper.get_followees(user=user, max_num=max_num)

        entries = [(user['id_str'], fol) for fol in followees_list]

        self.db.insert(table='followees',
                       cols_list=self.db.cols_followees,
                       val_list=entries,
                       ignore=ignore)

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
                                                        'value': user['id_str']}},
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

        return [user['id_str'] for user in members]

    def scrape_lists_of_user(self, user, list_type='m', max_num=100):
        assert list_type == 'm', 'Only mode implemented'
        if user['listed_count'] == 0:
            return []
        max_num = min(max_num, user['listed_count'])
        if self.use_cache:
            lists = self.db.select('members',
                                   filter_dict={'EQ': {'col': 'user_id_str',
                                                       'value': user['id_str']}})

            if lists is not None and len(lists) >= max_num:
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
        if seeker is None:
            return False
        tweets = self.scrape_user_activity(seeker, ignore=True)

        favs = self.scrape_user_activity(seeker, favs=True, ignore=True)
        followees = self.scrape_user_followees(seeker, ignore=True)
        # %%
        df_foll = self.scrape_many_users(id_str_list=followees)
        # %%
        df_foll = df_foll.sample(frac=1.0)
        for _, fol in df_foll.iterrows():
            if fol['protected'] == '1': continue
            if fol['statuses_count'] > 0:
                tweets = self.scrape_user_activity(fol, ignore=True)

        return True
    def add_topic(self, label, word_list):
        word_str = '___'.join(word_list)
        self.db.insert(table='topics',
                       cols_list=self.db.cols_topics,
                       val_list=tuple([label, word_str]))

        self.db.add_column('topics_lists', label, 'TINYINT DEFAULT -1')

    def add_topic_to_lists(self, topic_label):
        df_lists = self.db.select('lists')
        if self.use_cache:
            df_tl = self.db.select('topics_lists',
                                   filter_dict={'<>': {'col': topic_label,
                                                       'value': -1}
                                                })
            if df_tl is not None and len(df_tl) > 0:
                list_id_str_cache = list(df_tl['list_id_str'].unique())

                list_id_str = utools.list_substract(df_lists['id_str'].unique(), list_id_str_cache)
                df_lists = df_lists[df_lists.id_str.isin(list_id_str)]

        df_topics = self.db.select('topics')
        topic_words = df_topics[df_topics.label == topic_label].iloc[0]['description'].split('___')

        cols_list = ['list_id_str', topic_label]
        for _, my_list in df_lists.iterrows():
            text_processed = my_list['text_processed']
            is_in_topic = 0
            for word in topic_words:

                if word in my_list['text_processed']:
                    print(f"\n[{topic_label}] Text: {text_processed}")
                    is_in_topic = 1
                    break

            self.db.insert_update('topics_lists', cols_list, values=[my_list['id_str'], is_in_topic])

    def get_user_activity(self, user_id_str, init_dt, end_dt, favs=False):
        table = 'favs' if favs else 'tweets'

        init_dt = init_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_dt = end_dt.strftime("%Y-%m-%d %H:%M:%S")
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

        init_dt = init_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_dt = end_dt.strftime("%Y-%m-%d %H:%M:%S")

        df_wall = self.db.select('tweets',
                                 filter_dict={'IN': {'col': 'user_id_str',
                                                     'values': user_list},
                                              '>=': {'col': 'datetime',
                                                     'value': init_dt},
                                              '<=': {'col': 'datetime',
                                                     'value': end_dt}
                                              })

        df_wall = df_wall[~df_wall.tweet_user_id_str.eq(user_id_str)]
        return df_wall[~df_wall.type.eq('answer')]

    def get_wall_G(self, root, list_id_str, T=30, L=10, Q=0, sigma=8, use_followees=False):
        '''

        :param list_id_str:
        :param T: Number of days to look back from the list creation time
        :param L:  Minimum number of times listed to be considered an expert
        :param Q: Minimum number of times listed in Lists associated with the topic of the List
        :param sigma: Bandwitdth of the kernel in minutes

        :return:
        '''

        assert Q == 0

        # Get the List and the  Seeker
        my_list = self.scrape_list(list_id_str=list_id_str)
        seeker = self.scrape_user(id_str=my_list['user_id_str'])
        user_dir = os.path.join(root, f'{T}_{L}_{Q}_{sigma}', 'raw', my_list['user_id_str'])
        os.makedirs(user_dir, exist_ok=True)
        G_file = os.path.join(user_dir, f'G_{list_id_str}.pkl')
        if self.use_cache and os.path.exists(G_file):
            print(f'Getting wall from cache: {G_file}')
            G = utools.load_obj(G_file)
            return G, True
        # Set init and end time of wall
        end_dt = my_list['created_at']
        init_dt = end_dt - timedelta(days=T)

        # Get the wall
        success = self.scrape_wall(user_id_str=my_list['user_id_str'])
        if not success:
            print('\n ERROR SCRAPING THE WALL')
            return None, True
        df_wall = self.get_wall_from_db(my_list['user_id_str'],
                                        init_dt=init_dt,
                                        end_dt=end_dt
                                        )

        print(f"\nMin datetime: {df_wall['datetime'].min()}")
        print(f"Max datetime: {df_wall['datetime'].max()}\n")

        # Create the graph to store the wall and add the nodes
        G = nx.DiGraph()
        G.add_node(my_list['user_id_str'])
        G.add_nodes_from(list(df_wall['tweet_user_id_str'].unique()))

        # Get the users' information
        all_user_id_str_list = list(G.nodes())
        df_users = self.scrape_many_users(all_user_id_str_list)
        if len(df_users) != len(G.nodes()):
            id_str_1 = list(df_users['id_str'].unique())
            id_str_2 = list(G.nodes())
            print(utools.list_substract(id_str_2, id_str_1))
            # assert False

        # %% Add Follow edges

        if not use_followees:

            followees = self.scrape_user_followees(seeker)
            fol_in_wall = utools.list_intersection(all_user_id_str_list, followees)
            for fol in fol_in_wall:
                G.add_edge(seeker['id_str'], fol, follow=1)
        else:
            for _, u in df_users.iterrows():
                if u['friends_count'] == 0: continue
                followees = self.scrape_user_followees(u)
                fol_in_wall = utools.list_intersection(all_user_id_str_list, followees)
                for fol in fol_in_wall:
                    G.add_edge(u['id_str'], fol, follow=1)
        # %% Compute average number of times online at the same time

        df_t_seeker = self.get_user_activity(my_list['user_id_str'], init_dt, end_dt)
        df_l_seeker = self.get_user_activity(my_list['user_id_str'], init_dt, end_dt, favs=True)
        num_posts_seeker = len(df_t_seeker) + len(df_l_seeker)

        print(f"Number of posts of seeker: {num_posts_seeker}")

        seeker_dt_list = []

        if len(df_t_seeker) > 0:
            seeker_dt_list.extend(list(df_t_seeker['datetime'].map(lambda x: x.timestamp()) / 60))
        if len(df_l_seeker) > 0:
            seeker_dt_list.extend(list(df_l_seeker['datetime'].map(lambda x: x.timestamp()) / 60))
        G.nodes[seeker['id_str']]['dt_online'] = np.array(seeker_dt_list)

        for u_id_str, df_a in df_wall.groupby('tweet_user_id_str'):
            df_online = []
            if len(df_a) > 0:
                df_online = list(df_a['datetime'].map(lambda x: x.timestamp()) / 60)
            G.nodes[u_id_str]['dt_online'] = np.array(df_online)

        for id_str_i in all_user_id_str_list:
            for id_str_j in all_user_id_str_list:
                if id_str_i == id_str_j: continue

                dt_i = G.nodes[id_str_i]['dt_online']
                dt_j = G.nodes[id_str_j]['dt_online']

                diff_dt = dt_i[:, np.newaxis] - dt_j
                avg_num_online = np.sum(np.exp(-diff_dt ** 2 / sigma))

                if avg_num_online >= 1:
                    G.add_edge(id_str_i, id_str_j, avg_vis_tweets=avg_num_online)

        # %% Compute retweet, qtweets interactions

        for u_id_str, df_a in df_wall.groupby('user_id_str'):
            for i in ['qtweet', 'retweet']:
                df_i = df_a[df_a.type == i]
                for u_i, df_u_i in df_i.groupby('tweet_user_id_str'):
                    my_dict = {i: len(df_u_i)}
                    G.add_edge(u_id_str, u_i, **my_dict)

        # Qtweets, retweets and answers for the seeker
        for i in ['qtweet', 'retweet', 'answer']:
            df_i = df_t_seeker[df_t_seeker.type == i]
            for u_i, df_u_i in df_i.groupby('tweet_user_id_str'):
                my_dict = {i: len(df_u_i)}
                G.add_edge(seeker['id_str'], u_i, **my_dict)

        # Likes for the seeker
        for u_i, df_u_i in df_l_seeker.groupby('tweet_user_id_str'):
            my_dict = {'like': len(df_u_i)}
            G.add_edge(seeker['id_str'], u_i, **my_dict)
        # %% Add node attributtres

        for user_id_str, df_u in df_wall.groupby('tweet_user_id_str'):
            G.nodes[user_id_str]['rate_total'] = len(df_u) / T

        df_users['type'] = 'User'

        df_users.loc[df_users.listed_count >= L, 'type'] = 'Expert'

        members = self.scrape_list_members(list_id_str)

        df_users.loc[df_users.id_str.isin(members), 'type'] = 'Member'

        for _, u in df_users.iterrows():
            u_dict = u.to_dict()

            del u_dict['id']
            del u_dict['name']
            del u_dict['screen_name']
            del u_dict['description']
            del u_dict['url']
            del u_dict['protected']
            del u_dict['created_at']
            del u_dict['geo_enabled']
            del u_dict['profile_image_url']

            G.add_node(u['id_str'], **u_dict)

        G.nodes[seeker['id_str']]['type'] = 'Seeker'
        G.nodes[seeker['id_str']]['rate_total'] = num_posts_seeker / T

        # %% Add node attributes based on wall. Rate (events/tweet) of emojis, url, ...

        all_nodes = list(G.nodes())
        for u_id_str, df_u in df_wall.groupby('tweet_user_id_str'):
            if u_id_str not in all_nodes: continue
            G.nodes[u_id_str]['emojis_rate'] = np.sum(df_u['emojis']) / len(df_u)
            G.nodes[u_id_str]['hashtags_rate'] = np.sum(df_u['hashtags']) / len(df_u)
            G.nodes[u_id_str]['urls_rate'] = np.sum(df_u['urls']) / len(df_u)
            G.nodes[u_id_str]['mentions_rate'] = np.sum(df_u['mentions']) / len(df_u)
            docx = nt.TextFrame(text=' '.join(list(df_u['text_processed'].values)))

            G.nodes[u_id_str]['bow'] = docx.bow()

        df_u = pd.concat([df_l_seeker, df_t_seeker])
        G.nodes[seeker['id_str']]['emojis_rate'] = np.sum(df_u['emojis']) / len(df_u)
        G.nodes[seeker['id_str']]['hashtags_rate'] = np.sum(df_u['hashtags']) / len(df_u)
        G.nodes[seeker['id_str']]['urls_rate'] = np.sum(df_u['urls']) / len(df_u)
        G.nodes[seeker['id_str']]['mentions_rate'] = np.sum(df_u['mentions']) / len(df_u)
        docx = nt.TextFrame(text=' '.join(list(df_u['text_processed'].values)))

        G.nodes[seeker['id_str']]['bow'] = docx.bow()

        utools.save_obj(G_file, G)

        return G, False
