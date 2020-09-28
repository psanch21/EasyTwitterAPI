
# %%
from TwitterAPI import TwitterAPI

from EasyTwitterAPI.easy_twitter_api import get_credentials
from datetime import datetime
# %%
consumer_key, consumer_secret, access_token, access_token_secret = get_credentials(cred_file='local/credentials_premium.json')
api = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)


toDate = datetime.strptime('2020-05-01', '%Y-%m-%d').strftime("%Y%m%d%H%m")
# %%
endpoint = f'tweets/search/fullarchive/:prod'

query = {'query':'from:ortizsjesus to:skaasj', 'maxResults': 10}

r = api.request(endpoint, query)
for item in r:
    print(item['text'] if 'text' in item else item)


# %%
endpoint = f'tweets/search/fullarchive/:prod'
query = {'query':'from:elonmusk', 'maxResults': 10, 'toDate': toDate}

r = api.request(endpoint, query)
for item in r:
    print(item['text'] if 'text' in item else item)




# %%
endpoint = f'tweets/search/fullarchive/:prod'
query = {'query':'from:elonmusk', 'maxResults': 10}

r = api.request(endpoint, query)
for item in r:
    print(item['text'] if 'text' in item else item)



# %%

endpoint = f'statuses/user_timeline'

query = {'screen_name': 'IValeraM'}

r = api.request(endpoint, query)
i =0
for item in r:
    text = item['text'] if 'text' in item else item
    date = item['created_at']

    print(f"{i} {date} | {text}")
    i+=1









