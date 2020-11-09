
# %%
from TwitterAPI import TwitterAPI

from EasyTwitterAPI.easy_twitter_api import get_credentials
from datetime import datetime
from dateutil import parser
# %%
consumer_key, consumer_secret, access_token, access_token_secret = get_credentials(cred_file='local/credentials_premium_tpp.json')
api = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)


toDate = datetime.strptime('2020-05-01', '%Y-%m-%d').strftime("%Y%m%d%H%m")

fromDate = datetime.strptime('2020-05-01', '%Y-%m-%d').strftime("%Y%m%d%H%m")

# %%
endpoint = f'tweets/search/fullarchive/:awareness'

query = {'query':'from:PetarV_93', 'maxResults': 10, 'fromDate': fromDate}

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









# %%

endpoint = f'lists/memberships'

query = {'screen_name': 'yudapearl', 'count':10}

r = api.request(endpoint, query)
i =0
for item in r:
    print(r)
    break


# %%
lists = r.json()['lists']
my_dt = datetime.strptime('2020-10-05', '%Y-%m-%d')
for l in lists:
    print(l['created_at'])
    print(parser.parse(l['created_at']).replace(tzinfo=None) > my_dt)