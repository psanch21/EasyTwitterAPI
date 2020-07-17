# %%
from EasyTwitterAPI.easy_twitter_api import EasyTwitterAPI

import EasyTwitterAPI.utils.tools as utools
import datetime
import pandas as pd

scraper = EasyTwitterAPI(cred_file='local/credentials_tpp.json')

scraper.info_db(full=True)
# %% Get activity
# scraper.activate_cache(False)
tweets = scraper.get_user_activity_limited(screen_name='yudapearl',  since=datetime.datetime.strptime('2000-05-30', '%Y-%m-%d'))

df = utools.create_df_from_tweet_list(tweets)
# %% Update activity
twets = scraper.update_user_activity(screen_name='yudapearl')

# %% Get favorites
# scraper.activate_cache(False)
favorites = scraper.get_user_favorites(screen_name='yudapearl')


# %% Get user

user  = scraper.get_user(screen_name=['yudapearl', 'goodfellow_ian'])



# %% Get followees
members = scraper.get_followees(screen_name='yudapearl', max_num=5000)


# %% Get followers
members = scraper.get_followers(screen_name='yudapearl', max_num=10000)


# %% Get relationship
rela = scraper.get_relationship(source_screen_name='yudapearl', target_screen_name='fhuszar')


# %% Get lists of user
lists = scraper.get_lists_of_user(list_type='m', screen_name='yudapearl', max_num=100)


# %% Get list
l = scraper.get_list(list_id_str='1250703597429456902')


# %% Get members of lists
members = scraper.get_members_of_list(list_id_str='741805883991035904', max_num=100)



# %% Get replies to
replies = scraper.search_replies_to(screen_name='yudapearl', from_screen_name='heikalkhan')



# %% Load data

my_data = scraper.load_cache_data(collection='user', filter_={}, find_one=False)
df = pd.DataFrame.from_dict(my_data)