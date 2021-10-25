
# %%
from TwitterAPI import TwitterAPI

from EasyTwitterAPI.easy_twitter_api import get_credentials
from datetime import datetime
from dateutil import parser
# %%
consumer_key, consumer_secret, access_token, access_token_secret = get_credentials(cred_file='local/credentials_api.json')
api = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)


toDate = datetime.strptime('2020-05-01', '%Y-%m-%d').strftime("%Y%m%d%H%m")

fromDate = datetime.strptime('2000-05-01', '%Y-%m-%d').strftime("%Y%m%d%H%m")

# %%
endpoint = f'tweets/search/fullarchive/:awareness'
tweets_p = []
query = {'query':'from:PetarV_93', 'maxResults': 10, 'fromDate': fromDate}


r = api.request(endpoint, query)
for item in r:
    tweets_p.append(item)
    print(item['text'] if 'text' in item else item)


# %%
endpoint = f'tweets/search/fullarchive/:prod'
query = {'query':'from:elonmusk', 'maxResults': 10, 'toDate': toDate}

r = api.request(endpoint, query)
for item in r:
    print(item['text'] if 'text' in item else item)



# %%
endpoint = 'users/lookup'
query = {'screen_name':'elonmusk'}

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

from EasyTwitterAPI.utils.tools import clean_tweet
endpoint = f'statuses/user_timeline'

query = {'screen_name': 'IValeraM', 'tweet_mode': 'extended'}

i =0
tweets = []

users = {}
for _ in range(5):
    r = api.request(endpoint, query)

    for item in r:
        tweets.append(item)
        i += 1



# %%
from EasyTwitterAPI.utils.tools import clean_tweet
endpoint = f'statuses/lookup'

query = {'id': '1442974573247430661,1441478722398339072', 'tweet_mode': 'extended'}

i =0
tweets = []

users = {}
for _ in range(1):
    r = api.request(endpoint, query)

    for item in r:
        tweets.append(item)
        i += 1




# %%

from EasyTwitterAPI.utils.tools import clean_tweet
endpoint = f'favorites/list'

query = {'screen_name': 'elonmusk', 'tweet_mode': 'extended'}

i =0
tweets = []

users = {}
for _ in range(2):
    r = api.request(endpoint, query)

    for item in r:
        tweets.append(item)
        # tweets.append(clean_tweet(item, False,'123456'))

        i += 1
# %%

for t in tweets:
    if 'quoted_tweet' in t:
        print(t)

# %%

for t in tweets:
    if 'Floki' in t['full_text']:
        for key, value in t.items():
            print(f"{key}")
            print(f"\t{value}")

# %%

endpoint = f'lists/memberships'

query = {'screen_name': 'yudapearl', 'count':200}

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