# Easy Twitter API 
Python package for retrieving information from Twitter. This is a wrapper for  the 
packages [TwitterAPI](https://github.com/geduldig/TwitterAPI) and 
[twint](https://github.com/twintproject/twint) with improved functionalities:

- Additional methods that retrieve information of more complex queries.
- Cleaner way of calling the methods
- Store the data retrieved in MongoDB for quick access.


This is an ongoing project there are many functionalities (and documentation) to be added. Thit goes without saying, but contributions are highly appreciated.

## Installation

Download or clone this repo and from the command line execute: 

```
pip install -r requirements.txt
```

or 

```
python setup.py install
```

## How to use this package?
You just need to follow the next four steps.
#### Step 0: Get MongoDB

EasyTwitterAPI uses MongoDB to store the twitter data. We have tested the code in two settings: 
Local MongoDB and [Cloud MongoDB](https://www.mongodb.com/es).

If you opt for Local MongoDB,  EasyTwitterAPI assumes that a MongoDB instance is running on the default host and port. 
To install MongoDB please follow the [official guide](http://www.mongodb.org/display/DOCS/Getting+Started).

If you opt for Cloud MongoDB, you should i) create an account [here](https://www.mongodb.com/es),
 ii) create a database (there is a free version), iii) search for the `host` name under `Connect --> Connect your application`.
 The host should look something like
 ```
host = mongodb+srv://<user_admin>:<password>@cluster[...]mongodb.net/myF[...]
```
#### Step 1: Get your Twitter Developer credentials 

Go to [Twitter Developer](https://developer.twitter.com/en) and get your credentials, both the free and paid version are valid.
Then, save your credentials in your favourite folder of your computer. As an example, I have save them in a json file 
called ```credentials.json``` that looks as follows

```
{
    "ACCESS_KEY": "ggjhaglahdjkalghajk",
    "ACCESS_SECRET": "hfdajklfhadjkfha",
    "CONSUMER_KEY": "fhjdkalfhakjlfhasdkjlh",
    "CONSUMER_SECRET": "fhajklfhdajslfhda"
}
```


#### Step 2: Create the EasyTwitterAPI object
The main object only has one required parameter, the credential file of Step 1.
```
from easy_twitter_api import EasyTwitterAPI


scraper = EasyTwitterAPI(cred_file='credentials.json',
                         db_name='MY_TWITTER_DB',
                         sleep_secs=60 * 8,
                         host=host)
```


#### Step 3: Get data from Twitter
Now you are ready to get/scrape data from Twitter. The file ```test/test_all.py``` contains many examples of use. 
Some of the methods you can call are

```
# %% Get activity
tweets = scraper.get_user_activity_limited(screen_name='yudapearl',  since=datetime.datetime.strptime('2000-05-30', '%Y-%m-%d'))
# %% Get followees
members = scraper.get_followees(screen_name='yudapearl', max_num=5000)
```


## Further information
License for all files: [Apache License 2.0](LICENSE)

If you have any question, please don't hesitate to contact me at <psanch2103@gmail.com>