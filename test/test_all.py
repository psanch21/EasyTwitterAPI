# %%
from EasyTwitterAPI.easy_twitter_api import EasyTwitterAPI

import EasyTwitterAPI.utils.tools as utools
import datetime
import pandas as pd

from datetime import datetime
scraper = EasyTwitterAPI(cred_file='local/credentials_jl1.json',
                         db_name='easy_twitter_api')


scraper.set_cred_premium(
                         cred_file_premium='local/credentials_premium_mje1.json',
dev_label='awareness')

scraper.info_db(full=False)

scraper.activate_cache(True)
scraper.refresh_collection_names()

# %%
df_tweets = scraper.get_user_activity_limited(screen_name='PetarV_93', max_num=10)

# %%

df_tweets = scraper.update_user_activity_premium(min_dt=datetime.strptime('2020-05-14', '%Y-%m-%d'),
                                         screen_name='elonmusk')

# %%

df_tweets = scraper.update_user_activity(min_dt=datetime.strptime('2020-10-14', '%Y-%m-%d'),
                                         screen_name='PetarV_93')
 # %%
print(scraper.is_collected(what='activity', screen_name='yudapearl'))
# %% Get activity
# scraper.activate_cache(False)
df = scraper.get_user_activity_limited(screen_name='fhuszar')
twets = scraper.update_user_activity(min_dt=datetime.strptime('2020-10-14', '%Y-%m-%d'), screen_name='fhuszar')

# %% Get activity with twint
# scraper.activate_cache(False)
df = scraper.get_user_activity_limited(screen_name='elonmusk')


df = scraper.update_user_activity()
# %% Update activity
twets = scraper.update_user_activity(screen_name='yudapearl')

# %% Get favorites
favorites = scraper.get_user_favorites(screen_name='elonmusk')


favorites = scraper.update_favorites(min_dt=datetime.strptime('2020-10-14', '%Y-%m-%d'),
                                     screen_name='yudapearl')
# %% Get user

user  = scraper.get_user(screen_name=['yudapearl', 'goodfellow_ian'])



# %% Get followees
members = scraper.get_followers(screen_name='IvaleraM', max_num=5000)

members2 = scraper.get_many_users(user_id=members)

# %% Get followers
members = scraper.get_followers(screen_name='yudapearl', max_num=10000)


# %% Get relationship
rela = scraper.get_relationship(source_screen_name='yudapearl', target_screen_name='fhuszar')


# %% Get lists of user
lists = scraper.get_lists_of_user(list_type='m', screen_name='yudapearl', max_num=100)


# %% Get list
l = scraper.get_list(list_id_str='1297261774270431233')


# %% Get members of lists
members = scraper.get_members_of_list(list_id_str='1297261774270431233', max=200)

# %% Get  full lists
df_l = scraper.get_lists_of_user_full(list_type='m', screen_name='yudapearl')




# %% Get replies to
scraper.activate_cache(False)
replies = scraper.search_replies_to(screen_name='elonmusk', count=10)

# %% Get replies to Premium API
scraper.activate_cache(False)
replies = scraper.search_replies_to_2(screen_name='elonmusk',
                                      max_results=10,
                                      fromDate=datetime.strptime('2020-01-01', '%Y-%m-%d'))



# %%
df_lists = scraper.update_lists_of_user(list_type='m', min_dt=datetime.strptime('2020-10-01', '%Y-%m-%d'), full=True,  screen_name='yudapearl')

# %%

df_lists = scraper.get_lists_of_user_full(list_type='m', screen_name='yudapearl', since=datetime.strptime('2020-10-01', '%Y-%m-%d'))
# %% Load data

my_data = scraper.load_cache_data(collection='user', filter_={}, find_one=False)
df = pd.DataFrame.from_dict(my_data)


 # %% Refractor
import time
names = [n for n in scraper.db.list_collection_names() if 'tweets_' in n]
total_n = len(names)
tic = time.time()
for idx, collection_name in enumerate(names):
    if idx % 10 ==0:
        time_elapse = time.time() -tic
        print(f'{idx}/{total_n} {time_elapse}')
        tic =time.time()
    df_tweets = pd.DataFrame.from_dict(scraper.load_cache_data(collection=collection_name))
    if '_id' in df_tweets.columns: df_tweets.drop(columns=['_id'], inplace=True)
    df = df_tweets.apply(lambda tweet: pd.Series(utools.refractor_tweet(tweet)), axis=1)

    scraper.db.drop_collection(collection_name)
    collection = scraper.db[collection_name]
    collection.insert_many(list(df.T.to_dict().values()))



