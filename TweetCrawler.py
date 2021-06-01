import numpy as np
import pandas as pd
import pymongo
import time
import threading
import os
import json
import pprint
import datetime as DT
from datetime import date
from dotenv import load_dotenv

load_dotenv()

# Tweet Crawler Dependencies
from tweepy import OAuthHandler
import tweepy

class TweetCrawler:
    def __init__(self):
        self.credentials = None
        f = open('credentials.json',)
        self.credentials = json.load(f)
        self.place_id = None
        self.keywords = None
        self.main_db = None
        self.today = None
        self.min_net = None
        self.db_index = None
        self.db = []
        self.api = []
        self.HO = []
        self.HO_tweets = []
        self.toptweets = []
        self.raw_list = []
        self.users_list = []
        self.nodes_list = []
        self.jsons_list = []
        self.tweet_keys = ['tweet_id', 'user_id', 'user_screen_name', 'full_text', 'created_at', 'retweet_count', 'favorite_count', 
                        'retweeted', 'favorited', 'possibly_sensitive', 'truncated', 'in_reply_to_screen_name', 'in_reply_to_status_id_str', 
                        'in_reply_to_user_id_str', 'is_quote_status', 'quoted_status_id_str', 'quoted_user_id_str', 'retweeted_status_id_str', 
                        'retweeted_user_id_str', 'media', 'hashtags', 'lang', 'place', 'root_node', 'keyword', 'crawled_date', 'crawled_time']
        self.user_keys = ['user_id', 'created_at', 'total_tweets', 'followers_count', 'favourites_count', 'friends_count', 'listed_count', 
                        'following', 'location', 'protected', 'screen_name', 'verified', 'crawled_at']
        self.setup_api()
        self.setup_database()
        self.threads = list()
    

    def setup_api(self):
        #############################################################################
        #                                                                           #
        # Tweepy API setup.                                                         #
        #                                                                           #
        #############################################################################

        auth = OAuthHandler(self.credentials['CONSUMER_KEY_I'], self.credentials['CONSUMER_SECRET_I'])
        auth.set_access_token(self.credentials['ACCESS_KEY_I'], self.credentials['ACCESS_SECRET_I'])
        api1 = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

        auth = OAuthHandler(self.credentials['CONSUMER_KEY_II'], self.credentials['CONSUMER_SECRET_II'])
        auth.set_access_token(self.credentials['ACCESS_KEY_II'], self.credentials['ACCESS_SECRET_II'])
        api2 = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

        auth = OAuthHandler(self.credentials['CONSUMER_KEY_III'], self.credentials['CONSUMER_SECRET_III'])
        auth.set_access_token(self.credentials['ACCESS_KEY_III'], self.credentials['ACCESS_SECRET_III'])
        api3 = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

        self.api = [api1, api2, api3]


    def setup_database(self):
        #############################################################################
        #                                                                           #
        # Setting up database connection.                                           #
        #                                                                           #
        #############################################################################

        check_time = int(DT.datetime.now().strftime("%H"))

        client = pymongo.MongoClient(self.credentials['MONGODB_URI_I'])
        db_1 = client['THESIS']
        client = pymongo.MongoClient(self.credentials['MONGODB_URI_II'])
        db_2 = client['THESIS']
        client = pymongo.MongoClient(self.credentials['MONGODB_URI_III'])
        db_3 = client['THESIS']
        self.db = [db_1, db_2, db_3]

        if check_time >= 6 and check_time < 12:
            self.db_index = 0
        elif check_time >= 12 and check_time < 18:
            self.db_index = 1
        elif check_time >= 18:
            self.db_index = 2

        if self.main_db == None:
            main_client_db = pymongo.MongoClient(self.credentials['MONGODB_URI_IV'])
            self.main_db = main_client_db['THESIS']
        
        data = self.main_db['configs'].find_one({ "identifier": 'config' })
        self.place_id = data['place_id']
        self.keywords = data['keywords']
        self.today = data['date']
        self.HO = data['health_organizations']
        self.min_net = data['minimum_networks']
        
        today = DT.datetime.now()
        date_today = '{}-{}-{}'.format(today.strftime("%Y"), today.strftime("%m"), today.strftime("%d"))

        # reset top 10 if date changes
        if self.today != date_today:
            self.today = date_today
            self.main_db['configs'].update_one({ 'identifier' : 'config' }, { '$set': { 'date': date_today } })     # update current date in configs

            top10 = list(self.main_db['toptweets'].find({}, { '_id': 0 }))
            HO_tweets = list(self.main_db['health-organizations-tweets'].find({}, { '_id': 0 }))

            # insert the top 10 to the networks
            for top in top10:
                self.main_db['networks'].insert_one({
                    'root_node': top['tweet_id'],
                    'full_text': top['full_text'],
                    'tweet_id': top['tweet_id'],
                    'retweet_count': top['retweet_count'],
                    'screen_name': top['user_screen_name'],
                    'keyword': top['keyword'],
                    'root_date': top['crawled_date'],
                    'date': top['crawled_date']
                })

            # insert the yesterday's health-organizations-tweets to the networks
            for tw in HO_tweets:
                self.main_db['health-organizations-networks'].insert_one({
                    'root_node': tw['tweet_id'],
                    'full_text': tw['full_text'],
                    'tweet_id': tw['tweet_id'],
                    'retweet_count': tw['retweet_count'],
                    'screen_name': tw['user_screen_name'],
                    'keyword': tw['keyword'],
                    'root_date': tw['crawled_date'],
                    'date': tw['crawled_date']
                })

            # reset the top 10
            self.main_db['toptweets'].drop()    # reset the toptweets collections        

            # reset the health tweets
            self.main_db['health-organizations-tweets'].drop()    # reset the health-organization-tweets collections        


    def scrap_tweets_thread(self):
        #############################################################################
        #                                                                           #
        # Setting up thread for scraping tweets.                                    #
        #                                                                           #
        #############################################################################

        self.setup_database()
        print(self.keywords)

        stop_event = threading.Event()
        t = threading.Thread(target=self.scrap_tweets, args=(stop_event,), daemon=True)
        self.threads.append(t)
        self.threads[-1].start()


    def scrap_tweets(self, stop):
        #############################################################################
        #                                                                           #
        # Twitter scraping process with Tweepy API.                                 #
        # { Event } stop - event to stop threading                                  #
        #                                                                           #
        #############################################################################

        self.raw_list = []

        # Getting tweets based on keywords and location
        for key in self.keywords:
            start_run = time.time()
            try:
                tweets = tweepy.Cursor(self.api[0].search, q='{} place:{}'.format(key, self.place_id), since=self.today, tweet_mode='extended').items(1000)
                self.check_api_rate_limit(0, 'search', '/search/tweets')
                
                # Store these tweets into a python list
                tweet_list = [tweet for tweet in tweets]
                # Adding keyword
                for t in tweet_list:
                    t._json['keyword'] = key
                self.raw_list = np.concatenate((np.array(self.raw_list), np.array(tweet_list)))

                end_run = time.time()
                duration_run = round((end_run-start_run)/60, 2)

                print("No. of tweets scraped for keyword '{}' is {} | time {} mins".format(key, len(tweet_list), duration_run))
            except tweepy.TweepError as e:
                print(e.reason)
                print('Sleep for 5 more minutes...')
                time.sleep(300)
                continue
        
        tweet = None
        tweet_list = None
        self.construct_data(stop)


    def get_media(self, media):
        #############################################################################
        #                                                                           #
        # Getting the media type in a tweet.                                        #
        # { list } media - a list of media entities in twitter object               #
        #                                                                           #
        #############################################################################
        
        media_string = ''
        for i in range(len(media)):
            if i == 0:
                media_string += media[i]['type']
            else:
                media_string += ', {}'.format(media[i]['type'])
        
        return media_string


    def construct_data(self, stop):
        #############################################################################
        #                                                                           #
        # Formatting and converting the raw data in a more readable format.         #
        # { Event } stop - event to stop threading                                  #
        #                                                                           #
        #############################################################################

        today = DT.datetime.now()
        date_today = '{}-{}-{}'.format(today.strftime("%Y"), today.strftime("%m"), today.strftime("%d"))
        time_today = '{}:{}:{}'.format(today.strftime("%H"), today.strftime("%M"), today.strftime("%S"))
        crawl_time = '{} {}'.format(date_today, time_today)
        
        new_tweets = []
        new_jsons = []

        for tw in self.raw_list:
            tweet = tw._json
            user = tw.user._json
            new_jsons.append({ 'tweet': tweet, 'user': user, 'root_node': tweet['id_str'] })

            quoted_user_id_str = None
            if tweet['is_quote_status']:
                if 'quoted_status' in tweet.keys():
                    quoted_user_id_str = tweet['quoted_status']['user']['id_str']
                elif 'retweeted_status' in tweet.keys():
                    if 'quoted_status' in tweet['retweeted_status'].keys():
                        quoted_user_id_str = tweet['retweeted_status']['quoted_status']['user']['id_str']

            # Get Tweets
            tweet_data = {
                'tweet_id': tweet['id_str'],
                'user_id': tweet['user']['id_str'],
                'user_screen_name': tweet['user']['screen_name'],
                'full_text': tweet['full_text'],
                'created_at': tweet['created_at'],
                'retweet_count': tweet['retweet_count'],
                'favorite_count': tweet['favorite_count'],
                'retweeted': tweet['retweeted'],
                'favorited': tweet['favorited'],
                'possibly_sensitive': None if 'possibly_sensitive' not in tweet.keys() else tweet['possibly_sensitive'],
                'truncated': tweet['truncated'],
                'in_reply_to_screen_name': tweet['in_reply_to_screen_name'],
                'in_reply_to_status_id_str': tweet['in_reply_to_status_id_str'],
                'in_reply_to_user_id_str': tweet['in_reply_to_user_id_str'],
                'is_quote_status': tweet['is_quote_status'],
                'quoted_status_id_str': None if 'quoted_status_id_str' not in tweet.keys() else tweet['quoted_status_id_str'],
                'quoted_user_id_str': quoted_user_id_str,
                'retweeted_status_id_str': None if 'retweeted_status' not in tweet.keys() else tweet['retweeted_status']['id_str'],
                'retweeted_user_id_str': None if 'retweeted_status' not in tweet.keys() else tweet['retweeted_status']['user']['id_str'],
                'media': None if 'extended_entities' not in tweet.keys() else self.get_media(tweet['extended_entities']['media']),
                'hashtags': False if len(tweet['entities']['hashtags']) == 0 else True,
                'lang': tweet['lang'],
                'place': None if tweet['place'] == None else '{}, {}'.format(tweet['place']['full_name'], tweet['place']['country_code']),
                'root_node': tweet['id_str'],
                'keyword': tweet['keyword'],
                'crawled_date': date_today,
                'crawled_time': time_today
            }

            user_data = {
                'user_id': user['id_str'],
                'created_at': user['created_at'],
                'total_tweets': user['statuses_count'],
                'followers_count': user['followers_count'],
                'favourites_count': user['favourites_count'],
                'friends_count': user['friends_count'],
                'listed_count': user['listed_count'],
                'following': user['following'],
                'location': user['location'],
                'protected': user['protected'],
                'screen_name': user['screen_name'],
                'verified': user['verified'],
                'crawled_at': crawl_time
            }

            new_tweets.append(tweet_data)
            self.users_list.append(user_data)

        self.get_tweets_from_health_accounts()

        if len(new_jsons) > 0 or len(new_tweets) > 0:
            self.get_first_degree_network(new_tweets, new_jsons, stop)
        else:
            print('No new tweets found!')


    def format_date(self, date_time):
        #############################################################################
        #                                                                           #
        # Returns the YYYY-MM-DD format of the created_at date                      #
        # { string } date_time - tweet's created_at value                           #
        #                                                                           #
        #############################################################################

        formatted = DT.datetime.strptime(date_time, '%a %b %d %H:%M:%S %z %Y')
        return '{}-{}-{}'.format(formatted.strftime("%Y"), formatted.strftime("%m"), formatted.strftime("%d"))


    def get_tweets_from_health_accounts(self):
        #############################################################################
        #                                                                           #
        # Twitter scraping process for health accounts.                             #
        #                                                                           #
        #############################################################################
        
        currentHO_tweets = list(self.main_db['health-organizations-tweets'].find({}, { '_id': 0 }))

        today = DT.datetime.now()
        date_today = '{}-{}-{}'.format(today.strftime("%Y"), today.strftime("%m"), today.strftime("%d"))
        time_today = '{}:{}:{}'.format(today.strftime("%H"), today.strftime("%M"), today.strftime("%S"))
        crawl_time = '{} {}'.format(date_today, time_today)
        HO_raw_tweets = []

        # Getting tweets based on health organization accounts
        for org in self.HO:
            start_run = time.time()
            try:
                tweets = tweepy.Cursor(self.api[1].user_timeline, screen_name=org, since=self.today, tweet_mode='extended').items(200)
                self.check_api_rate_limit(1, 'statuses', '/statuses/user_timeline')

                # Store these tweets into a python list
                tweet_list = [tweet for tweet in tweets]
                # Getting only today's tweets
                tweet_list = [t for t in tweet_list if self.format_date(t._json['created_at']) == self.today]
                # Adding keyword
                for t in tweet_list:
                    keys = [k for k in self.keywords if k.lower() in t._json['full_text'].lower().split(' ') or '#{}'.format(k.lower()) in t._json['full_text'].lower().split(' ')]
                    t._json['keyword'] = None if len(keys) == 0 else keys[0]
                HO_raw_tweets = np.concatenate((np.array(HO_raw_tweets), np.array(tweet_list)))

                end_run = time.time()
                duration_run = round((end_run-start_run)/60, 2)

                print("No. of tweets scraped from '{}' is {} | time {} mins".format(org, len(tweet_list), duration_run))
            except tweepy.TweepError as e:
                print(e.reason)
                print('Sleep for 5 more minutes...')
                time.sleep(300)
                continue
        
        HO_tweets = []

        for tw in HO_raw_tweets:
            tweet = tw._json
            user = tw.user._json
            json_data = { 'tweet': tweet, 'user': user, 'root_node': tweet['id_str'] }

            quoted_user_id_str = None
            if tweet['is_quote_status']:
                if 'quoted_status' in tweet.keys():
                    quoted_user_id_str = tweet['quoted_status']['user']['id_str']
                elif 'retweeted_status' in tweet.keys():
                    if 'quoted_status' in tweet['retweeted_status'].keys():
                        quoted_user_id_str = tweet['retweeted_status']['quoted_status']['user']['id_str']

            # Get Tweets
            tweet_data = {
                'tweet_id': tweet['id_str'],
                'user_id': tweet['user']['id_str'],
                'user_screen_name': tweet['user']['screen_name'],
                'full_text': tweet['full_text'],
                'created_at': tweet['created_at'],
                'retweet_count': tweet['retweet_count'],
                'favorite_count': tweet['favorite_count'],
                'retweeted': tweet['retweeted'],
                'favorited': tweet['favorited'],
                'possibly_sensitive': None if 'possibly_sensitive' not in tweet.keys() else tweet['possibly_sensitive'],
                'truncated': tweet['truncated'],
                'in_reply_to_screen_name': tweet['in_reply_to_screen_name'],
                'in_reply_to_status_id_str': tweet['in_reply_to_status_id_str'],
                'in_reply_to_user_id_str': tweet['in_reply_to_user_id_str'],
                'is_quote_status': tweet['is_quote_status'],
                'quoted_status_id_str': None if 'quoted_status_id_str' not in tweet.keys() else tweet['quoted_status_id_str'],
                'quoted_user_id_str': quoted_user_id_str,
                'retweeted_status_id_str': None if 'retweeted_status' not in tweet.keys() else tweet['retweeted_status']['id_str'],
                'retweeted_user_id_str': None if 'retweeted_status' not in tweet.keys() else tweet['retweeted_status']['user']['id_str'],
                'media': None if 'extended_entities' not in tweet.keys() else self.get_media(tweet['extended_entities']['media']),
                'hashtags': False if len(tweet['entities']['hashtags']) == 0 else True,
                'lang': tweet['lang'],
                'place': None if tweet['place'] == None else '{}, {}'.format(tweet['place']['full_name'], tweet['place']['country_code']),
                'root_node': tweet['id_str'],
                'keyword': None,
                'crawled_date': date_today,
                'crawled_time': time_today
            }

            user_data = {
                'user_id': user['id_str'],
                'created_at': user['created_at'],
                'total_tweets': user['statuses_count'],
                'followers_count': user['followers_count'],
                'favourites_count': user['favourites_count'],
                'friends_count': user['friends_count'],
                'listed_count': user['listed_count'],
                'following': user['following'],
                'location': user['location'],
                'protected': user['protected'],
                'screen_name': user['screen_name'],
                'verified': user['verified'],
                'crawled_at': crawl_time
            }
            
            if any(tweet_data['tweet_id'] == t['tweet_id'] for t in currentHO_tweets):
                self.main_db['health-organizations-tweets'].delete_one({ 'tweet_id': tweet_data['tweet_id'] })
            
            HO_tweets.append(tweet_data)

            self.db[self.db_index]['HO Nodes {}'.format(date_today)].insert_one(tweet_data)
            self.db[self.db_index]['Users {}'.format(date_today)].insert_one(user_data)
            self.db[self.db_index]['JSONs {}'.format(date_today)].insert_one(json_data)
            self.main_db['health-organizations-tweets'].insert_one(tweet_data)

        self.HO_tweets = pd.DataFrame.from_dict(HO_tweets)


    def get_first_degree_network(self, new_tweets, new_jsons, stop):
        #############################################################################
        #                                                                           #
        # Getting the retweets from the tweets in the list.                         #
        # { list } new_tweets - list of new extracted tweets                        #
        # { list } new_jsons - list of new extracted tweets in jsons                #
        # { Event } stop - event to stop threading                                  #
        #                                                                           #
        #############################################################################

        # get the current list of top tweets and networks in database
        top10 = list(self.main_db['toptweets'].find({}, { '_id': 0 }))
        networks = list(self.main_db['networks'].find({}, { '_id': 0 }))

        today = DT.datetime.now()
        date_today = '{}-{}-{}'.format(today.strftime("%Y"), today.strftime("%m"), today.strftime("%d"))
        time_today = '{}:{}:{}'.format(today.strftime("%H"), today.strftime("%M"), today.strftime("%S"))
        crawl_time = '{} {}'.format(date_today, time_today)
        dataframe = pd.DataFrame.from_dict(new_tweets)
        dataframe = dataframe.sort_values(['retweet_count', 'favorite_count'], ascending=False)
        dataframe = dataframe.drop_duplicates(subset='tweet_id', keep='first', inplace=False)
        dataframe = dataframe.head(10)
        dataframe = dataframe.reset_index(drop=True)
        self.toptweets = dataframe

        new_top10 = dataframe.to_dict('records')
        removed_toptweets = []

        # updating the toptweets
        for top in top10:
            if not any(n_top['tweet_id'] == top['tweet_id'] for n_top in new_top10):        # check if the top 10 list is changed
                removed_toptweets.append(top)                                               # append the tweet for removal of nodes

        # Delete removed Networks, JSONs, and Nodes from database
        for removed in removed_toptweets:
            children_nodes = [n for n in networks if n['root_node'] == removed['tweet_id']]

            if len(children_nodes) < self.min_net:
                self.main_db['networks'].delete_many({ 'root_node': removed['tweet_id'] })
                # for db in self.db:
                #     'JSONs {}'.format(self.today) in db.list_collection_names() and db['JSONs {}'.format(self.today)].delete_many({ 'root_node': removed_id })
                #     'Nodes {}'.format(self.today) in db.list_collection_names() and db['Nodes {}'.format(self.today)].delete_many({ 'root_node': removed_id })      
            else:
                self.main_db['networks'].insert_one({
                    'root_node': removed['tweet_id'],
                    'full_text': removed['full_text'],
                    'tweet_id': removed['tweet_id'],
                    'retweet_count': removed['retweet_count'],
                    'screen_name': removed['user_screen_name'],
                    'keyword': removed['keyword'],
                    'root_date': removed['crawled_date'],
                    'date': removed['crawled_date']
                })

        # processing the new jsons and users
        new_jsons = [j for j in new_jsons if any(j['root_node'] == n_top['tweet_id'] for n_top in new_top10)]
        users = pd.DataFrame(data=self.users_list, columns=self.user_keys)
        users = users.sort_values('user_id', ascending=False).drop_duplicates('user_id').sort_index()
        users = users.to_dict('records')

        # update JSONs and Nodes list
        self.db[self.db_index]['JSONs {}'.format(self.today)].insert_many(new_jsons)
        self.db[self.db_index]['Nodes {}'.format(self.today)].insert_many(new_top10)
        self.db[self.db_index]['Users {}'.format(self.today)].insert_many(users)

        new_users = [u for u in users if any(u['user_id'] == ntop['user_id'] for ntop in new_top10)]
        self.users_list = []

        # Update current Top 10 in database
        if 'toptweets' in self.main_db.list_collection_names():  # db exists in the database
            self.main_db['toptweets'].drop()

        self.main_db['toptweets'].insert_many(new_top10)
        self.get_parent_root()

        # get the updated networks
        networks = list(self.main_db['networks'].find({}, { '_id': 0 }))

        self.get_retweets(networks, stop)


    def check_api_rate_limit(self, index, api_ref, api_id):
        #############################################################################
        #                                                                           #
        # Check api rate limit.                                                     #
        # { int } index - api index                                                 #
        # { string } api_ref - api reference                                        #
        # { string } api_id - specific api                                          #
        #                                                                           #
        #############################################################################

        rate_limit = self.api[index].rate_limit_status()
        limit = rate_limit['resources'][api_ref][api_id]['limit']
        remaining = rate_limit['resources'][api_ref][api_id]['remaining']
        print('API {} [{}] - Requests remaining: {} out of {}'.format(index + 1, api_id, remaining, limit))


    def get_parent_root(self):
        #############################################################################
        #                                                                           #
        # Getting the parent nodes of top tweets if applicable.                     #
        #                                                                           #
        #############################################################################

        top10 = list(self.main_db['toptweets'].find({}, { '_id': 0 }))
        today = DT.datetime.now()
        date_today = '{}-{}-{}'.format(today.strftime("%Y"), today.strftime("%m"), today.strftime("%d"))
        time_today = '{}:{}:{}'.format(today.strftime("%H"), today.strftime("%M"), today.strftime("%S"))
        crawl_time = '{} {}'.format(date_today, time_today)
        parent_tweets = []

        for top in top10:
            start_run = time.time()
            try:
                if top['in_reply_to_status_id_str'] != None:
                    tweet = self.api[0].get_status(int(top['in_reply_to_status_id_str']), tweet_mode='extended')
                    self.check_api_rate_limit(0, 'statuses', '/statuses/show/:id')
                    parent_tweets.append(tweet)
                if top['quoted_status_id_str'] != None:
                    tweet = self.api[0].get_status(int(top['quoted_status_id_str']), tweet_mode='extended')
                    self.check_api_rate_limit(0, 'statuses', '/statuses/show/:id')
                    parent_tweets.append(tweet)
                if top['retweeted_status_id_str'] != None:
                    tweet = self.api[0].get_status(int(top['retweeted_status_id_str']), tweet_mode='extended')
                    self.check_api_rate_limit(0, 'statuses', '/statuses/show/:id')
                    parent_tweets.append(tweet)
            except tweepy.TweepError as e:
                print(e.reason)
                continue
        
        for tw in parent_tweets:
            tweet = tw._json
            user = tw.user._json
            json_data = { 'tweet': tweet, 'user': user, 'root_node': tweet['id_str'] }
            
            keys = [k for k in self.keywords if k.lower() in tweet['full_text'].lower()]
            keyword = None if len(keys) == 0 else keys[0]

            quoted_user_id_str = None
            if tweet['is_quote_status']:
                if 'quoted_status' in tweet.keys():
                    quoted_user_id_str = tweet['quoted_status']['user']['id_str']
                elif 'retweeted_status' in tweet.keys():
                    if 'quoted_status' in tweet['retweeted_status'].keys():
                        quoted_user_id_str = tweet['retweeted_status']['quoted_status']['user']['id_str']

            # Get Tweets
            tweet_data = {
                'tweet_id': tweet['id_str'],
                'user_id': tweet['user']['id_str'],
                'user_screen_name': tweet['user']['screen_name'],
                'full_text': tweet['full_text'],
                'created_at': tweet['created_at'],
                'retweet_count': tweet['retweet_count'],
                'favorite_count': tweet['favorite_count'],
                'retweeted': tweet['retweeted'],
                'favorited': tweet['favorited'],
                'possibly_sensitive': None if 'possibly_sensitive' not in tweet.keys() else tweet['possibly_sensitive'],
                'truncated': tweet['truncated'],
                'in_reply_to_screen_name': tweet['in_reply_to_screen_name'],
                'in_reply_to_status_id_str': tweet['in_reply_to_status_id_str'],
                'in_reply_to_user_id_str': tweet['in_reply_to_user_id_str'],
                'is_quote_status': tweet['is_quote_status'],
                'quoted_status_id_str': None if 'quoted_status_id_str' not in tweet.keys() else tweet['quoted_status_id_str'],
                'quoted_user_id_str': quoted_user_id_str,
                'retweeted_status_id_str': None if 'retweeted_status' not in tweet.keys() else tweet['retweeted_status']['id_str'],
                'retweeted_user_id_str': None if 'retweeted_status' not in tweet.keys() else tweet['retweeted_status']['user']['id_str'],
                'media': None if 'extended_entities' not in tweet.keys() else self.get_media(tweet['extended_entities']['media']),
                'hashtags': False if len(tweet['entities']['hashtags']) == 0 else True,
                'lang': tweet['lang'],
                'place': None if tweet['place'] == None else '{}, {}'.format(tweet['place']['full_name'], tweet['place']['country_code']),
                'root_node': tweet['id_str'],
                'keyword': keyword,
                'crawled_date': date_today,
                'crawled_time': time_today
            }

            user_data = {
                'user_id': user['id_str'],
                'created_at': user['created_at'],
                'total_tweets': user['statuses_count'],
                'followers_count': user['followers_count'],
                'favourites_count': user['favourites_count'],
                'friends_count': user['friends_count'],
                'listed_count': user['listed_count'],
                'following': user['following'],
                'location': user['location'],
                'protected': user['protected'],
                'screen_name': user['screen_name'],
                'verified': user['verified'],
                'crawled_at': crawl_time
            }

            self.db[self.db_index]['Nodes {}'.format(date_today)].insert_one(tweet_data)
            self.db[self.db_index]['Users {}'.format(date_today)].insert_one(user_data)
            self.db[self.db_index]['JSONs {}'.format(date_today)].insert_one(json_data)


    def get_retweets(self, networks, stop):
        #############################################################################
        #                                                                           #
        # Getting the retweets from the tweets in the top tweets.                   #
        # { list } networks - list of current network in the database               #
        # { Event } stop - event to stop threading                                  #
        #                                                                           #
        #############################################################################

        today = DT.datetime.now()
        date_today = '{}-{}-{}'.format(today.strftime("%Y"), today.strftime("%m"), today.strftime("%d"))
        time_today = '{}:{}:{}'.format(today.strftime("%H"), today.strftime("%M"), today.strftime("%S"))
        crawl_time = '{} {}'.format(date_today, time_today)

        retweets = []
        retweets_data = []
        idx = 0

        while idx < len(self.toptweets.index):
            retweet_count = int(self.toptweets.iloc[idx]['retweet_count'])
            if retweet_count > 0:
                # print('Count: {} | Index: {}'.format(count, idx))
                tweet_id = int(self.toptweets.iloc[idx]['tweet_id'])

                try:
                    rt = self.api[0].retweets(tweet_id, retweet_count)
                    self.check_api_rate_limit(0, 'statuses', '/statuses/retweets/:id')

                    print('Retweets of [{}]: {} of {} | {:.2f}%'.format(tweet_id, len(rt), retweet_count, len(rt)*100/retweet_count))
                    if len(rt) > 0:
                        for i in range(len(rt)):
                            retweets_data.append({
                                'full_text': self.toptweets.iloc[idx]['full_text'],
                                'tweet_id': self.toptweets.iloc[idx]['tweet_id'],
                                'screen_name': self.toptweets.iloc[idx]['user_screen_name'],
                                'keyword': self.toptweets.iloc[idx]['keyword']
                            })
                        retweets.extend(rt)                        
                    
                    idx += 1
                except tweepy.TweepError as e:
                    print(e)
                    idx += 1
                    continue
            else:
                idx += 1
        
        print('Total retweets: {}'.format(len(retweets)))

        for tw in range(len(retweets)):
            tweet = retweets[tw]._json
            user = retweets[tw].user._json
            json_data = { 'tweet': tweet, 'user': user, 'root_node': retweets_data[tw]['tweet_id'] }

            text = 'RT @{}: {}'.format(retweets_data[tw]['screen_name'], retweets_data[tw]['full_text'])

            quoted_user_id_str = None
            if tweet['is_quote_status']:
                if 'quoted_status' in tweet.keys():
                    quoted_user_id_str = tweet['quoted_status']['user']['id_str']
                elif 'retweeted_status' in tweet.keys():
                    if 'quoted_status' in tweet['retweeted_status'].keys():
                        quoted_user_id_str = tweet['retweeted_status']['quoted_status']['user']['id_str']

            # Get Tweets
            tweet_data = {
                'tweet_id': tweet['id_str'],
                'user_id': tweet['user']['id_str'],
                'user_screen_name': tweet['user']['screen_name'],
                'full_text': text,
                'created_at': tweet['created_at'],
                'retweet_count': tweet['retweet_count'],
                'favorite_count': tweet['favorite_count'],
                'retweeted': tweet['retweeted'],
                'favorited': tweet['favorited'],
                'possibly_sensitive': None if 'possibly_sensitive' not in tweet.keys() else tweet['possibly_sensitive'],
                'truncated': tweet['truncated'],
                'in_reply_to_screen_name': tweet['in_reply_to_screen_name'],
                'in_reply_to_status_id_str': tweet['in_reply_to_status_id_str'],
                'in_reply_to_user_id_str': tweet['in_reply_to_user_id_str'],
                'is_quote_status': tweet['is_quote_status'],
                'quoted_status_id_str': None if 'quoted_status_id_str' not in tweet.keys() else tweet['quoted_status_id_str'],
                'quoted_user_id_str': quoted_user_id_str,
                'retweeted_status_id_str': None if 'retweeted_status' not in tweet.keys() else tweet['retweeted_status']['id_str'],
                'retweeted_user_id_str': None if 'retweeted_status' not in tweet.keys() else tweet['retweeted_status']['user']['id_str'],
                'media': None if 'extended_entities' not in tweet.keys() else self.get_media(tweet['extended_entities']['media']),
                'hashtags': False if len(tweet['entities']['hashtags']) == 0 else True,
                'lang': tweet['lang'],
                'place': None,
                'root_node': retweets_data[tw]['tweet_id'],
                'keyword': retweets_data[tw]['keyword'],
                'crawled_date': date_today,
                'crawled_time': time_today
            }

            user_data = {
                'user_id': user['id_str'],
                'created_at': user['created_at'],
                'total_tweets': user['statuses_count'],
                'followers_count': user['followers_count'],
                'favourites_count': user['favourites_count'],
                'friends_count': user['friends_count'],
                'listed_count': user['listed_count'],
                'following': user['following'],
                'location': user['location'],
                'protected': user['protected'],
                'screen_name': user['screen_name'],
                'verified': user['verified'],
                'crawled_at': crawl_time
            }

            self.db[self.db_index]['Nodes {}'.format(date_today)].insert_one(tweet_data)
            self.db[self.db_index]['Users {}'.format(date_today)].insert_one(user_data)
            self.db[self.db_index]['JSONs {}'.format(date_today)].insert_one(json_data)

        print('Uploaded {} retweets!'.format(len(retweets)))

        retweets = []
        retweets_data = []
        self.get_replies_and_quote_tweets(networks, stop)
    

    def get_replies_and_quote_tweets(self, networks, stop):
        #############################################################################
        #                                                                           #
        # Getting the replies and quoted tweets from the tweets in the top tweets.  #
        # { list } networks - list of current network in the database               #
        # { Event } stop - event to stop threading                                  #
        #                                                                           #
        #############################################################################

        today = DT.datetime.now()
        date_today = '{}-{}-{}'.format(today.strftime("%Y"), today.strftime("%m"), today.strftime("%d"))
        time_today = '{}:{}:{}'.format(today.strftime("%H"), today.strftime("%M"), today.strftime("%S"))
        crawl_time = '{} {}'.format(date_today, time_today)

        tweet_threads = []
        replies = []
        quote_tweets = []
        new_networks = []
        threads_data = []
        idx = 0

        while idx < len(self.toptweets.index):
            # print('Count: {} | Index: {}'.format(count, idx))
            screen_name = self.toptweets.iloc[idx]['user_screen_name']
            tweet_id = int(self.toptweets.iloc[idx]['tweet_id'])
            keyword = self.toptweets.iloc[idx]['keyword']

            try:
                u = self.api[0].get_user(screen_name)
                u = u._json

                if not u['protected']:
                    extracted = tweepy.Cursor(self.api[0].search, q='{}'.format(screen_name), since_id=tweet_id, tweet_mode='extended').items(500)
                    self.check_api_rate_limit(0, 'search', '/search/tweets')

                    tw = [e for e in extracted]
                    print('Threads of [{}]: {} tweets'.format(tweet_id, len(tw)))
                    if len(tw) > 0:
                        for i in range(len(tw)):
                            threads_data.append({
                                'tweet_id': str(tweet_id),
                                'screen_name': screen_name,
                                'full_text': self.toptweets.iloc[idx]['full_text'],
                                'keyword': keyword
                            })
                        tweet_threads.extend(tw)
                
                idx += 1
            except tweepy.TweepError as e:
                print(e)
                idx += 1
                continue

        for tw in range(len(tweet_threads)):
            tweet = tweet_threads[tw]._json
            user = tweet_threads[tw].user._json
            json_data = { 'tweet': tweet, 'user': user, 'root_node': threads_data[tw]['tweet_id'] }

            if 'retweeted_status' not in tweet.keys():
                quoted_user_id_str = None
                if tweet['is_quote_status']:
                    if 'quoted_status' in tweet.keys():
                        quoted_user_id_str = tweet['quoted_status']['user']['id_str']
                    elif 'retweeted_status' in tweet.keys():
                        if 'quoted_status' in tweet['retweeted_status'].keys():
                            quoted_user_id_str = tweet['retweeted_status']['quoted_status']['user']['id_str']
                            
                # Get Tweets
                tweet_data = {
                    'tweet_id': tweet['id_str'],
                    'user_id': tweet['user']['id_str'],
                    'user_screen_name': tweet['user']['screen_name'],
                    'full_text': tweet['full_text'],
                    'created_at': tweet['created_at'],
                    'retweet_count': tweet['retweet_count'],
                    'favorite_count': tweet['favorite_count'],
                    'retweeted': tweet['retweeted'],
                    'favorited': tweet['favorited'],
                    'possibly_sensitive': None if 'possibly_sensitive' not in tweet.keys() else tweet['possibly_sensitive'],
                    'truncated': tweet['truncated'],
                    'in_reply_to_screen_name': tweet['in_reply_to_screen_name'],
                    'in_reply_to_status_id_str': tweet['in_reply_to_status_id_str'],
                    'in_reply_to_user_id_str': tweet['in_reply_to_user_id_str'],
                    'is_quote_status': tweet['is_quote_status'],
                    'quoted_status_id_str': None if 'quoted_status_id_str' not in tweet.keys() else tweet['quoted_status_id_str'],
                    'quoted_user_id_str': quoted_user_id_str,
                    'retweeted_status_id_str': None if 'retweeted_status' not in tweet.keys() else tweet['retweeted_status']['id_str'],
                    'retweeted_user_id_str': None if 'retweeted_status' not in tweet.keys() else tweet['retweeted_status']['user']['id_str'],
                    'media': None if 'extended_entities' not in tweet.keys() else self.get_media(tweet['extended_entities']['media']),
                    'hashtags': False if len(tweet['entities']['hashtags']) == 0 else True,
                    'lang': tweet['lang'],
                    'place': None,
                    'root_node': threads_data[tw]['tweet_id'],
                    'keyword': threads_data[tw]['keyword'],
                    'crawled_date': date_today,
                    'crawled_time': time_today
                }

                user_data = {
                    'user_id': user['id_str'],
                    'created_at': user['created_at'],
                    'total_tweets': user['statuses_count'],
                    'followers_count': user['followers_count'],
                    'favourites_count': user['favourites_count'],
                    'friends_count': user['friends_count'],
                    'listed_count': user['listed_count'],
                    'following': user['following'],
                    'location': user['location'],
                    'protected': user['protected'],
                    'screen_name': user['screen_name'],
                    'verified': user['verified'],
                    'crawled_at': crawl_time
                }

                # getting replies only
                if tweet_data['in_reply_to_status_id_str'] == threads_data[tw]['tweet_id']:
                    # print('{} | {}'.format(tweet_data['in_reply_to_status_id_str'], threads_data[tw]['tweet_id']))
                    new_networks.append({
                        # 'created_at': tweet_data['created_at'],
                        'root_node': threads_data[tw]['tweet_id'],
                        'full_text': tweet_data['full_text'],
                        'tweet_id': tweet_data['tweet_id'],
                        'retweet_count': tweet_data['retweet_count'],
                        'screen_name': user_data['screen_name'],
                        'keyword': tweet_data['keyword'],
                        'root_date': date_today,
                        'date': date_today
                    })
                    self.db[self.db_index]['Nodes {}'.format(date_today)].insert_one(tweet_data)
                    self.db[self.db_index]['Users {}'.format(date_today)].insert_one(user_data)
                    self.db[self.db_index]['JSONs {}'.format(date_today)].insert_one(json_data)
                
                # getting quote tweets only
                if tweet_data['is_quote_status'] == True and tweet_data['quoted_status_id_str'] == threads_data[tw]['tweet_id']:
                    new_networks.append({
                        'root_node': threads_data[tw]['tweet_id'],
                        'full_text': tweet_data['full_text'],
                        'tweet_id': tweet_data['tweet_id'],
                        'retweet_count': tweet_data['retweet_count'],
                        'screen_name': user_data['screen_name'],
                        'keyword': tweet_data['keyword'],
                        'root_date': date_today,
                        'date': date_today
                    })
                    self.db[self.db_index]['Nodes {}'.format(date_today)].insert_one(tweet_data)
                    self.db[self.db_index]['Users {}'.format(date_today)].insert_one(user_data)
                    self.db[self.db_index]['JSONs {}'.format(date_today)].insert_one(json_data)

        # add the new networks to database
        count = 0
        for net in new_networks:
            if any(net['tweet_id'] == n['tweet_id'] for n in networks):
                self.main_db['networks'].update_one({ 'tweet_id': net['tweet_id'] }, { '$set': { 'retweet_count': net['retweet_count'] } })
            else:
                count += 1
                self.main_db['networks'].insert_one(net)

        print('New nodes: {}'.format(count))
        print('Data uploaded!')

        tweet_threads = []
        replies = []
        quote_tweets = []
        new_networks = []
        threads_data = []
        self.get_HO_retweets(stop)


    def get_HO_retweets(self, stop):
        #############################################################################
        #                                                                           #
        # Getting the retweets from the tweets in health-organizations-tweets.      #
        # { Event } stop - event to stop threading                                  #
        #                                                                           #
        #############################################################################

        networks = list(self.main_db['health-organizations-networks'].find({}, { '_id': 0 }))

        today = DT.datetime.now()
        date_today = '{}-{}-{}'.format(today.strftime("%Y"), today.strftime("%m"), today.strftime("%d"))
        time_today = '{}:{}:{}'.format(today.strftime("%H"), today.strftime("%M"), today.strftime("%S"))
        crawl_time = '{} {}'.format(date_today, time_today)

        retweets = []
        retweets_data = []
        idx = 0

        while idx < len(self.HO_tweets.index):
            retweet_count = int(self.HO_tweets.iloc[idx]['retweet_count'])
            if retweet_count > 0:
                # print('Count: {} | Index: {}'.format(count, idx))
                tweet_id = int(self.HO_tweets.iloc[idx]['tweet_id'])

                try:
                    rt = self.api[1].retweets(tweet_id, retweet_count)
                    self.check_api_rate_limit(1, 'statuses', '/statuses/retweets/:id')

                    print('Retweets of [{}]: {} of {} | {:.2f}%'.format(tweet_id, len(rt), retweet_count, len(rt)*100/retweet_count))
                    if len(rt) > 0:
                        for i in range(len(rt)):
                            retweets_data.append({
                                'full_text': self.HO_tweets.iloc[idx]['full_text'],
                                'tweet_id': self.HO_tweets.iloc[idx]['tweet_id'],
                                'screen_name': self.HO_tweets.iloc[idx]['user_screen_name'],
                                'keyword': self.HO_tweets.iloc[idx]['keyword']
                            })
                        retweets.extend(rt)                        
                    
                    idx += 1
                except tweepy.TweepError as e:
                    print(e)
                    idx += 1
                    continue
            else:
                idx += 1
        
        print('Total retweets from health organizations: {}'.format(len(retweets)))

        for tw in range(len(retweets)):
            tweet = retweets[tw]._json
            user = retweets[tw].user._json
            json_data = { 'tweet': tweet, 'user': user, 'root_node': retweets_data[tw]['tweet_id'] }

            text = 'RT @{}: {}'.format(retweets_data[tw]['screen_name'], retweets_data[tw]['full_text'])

            quoted_user_id_str = None
            if tweet['is_quote_status']:
                if 'quoted_status' in tweet.keys():
                    quoted_user_id_str = tweet['quoted_status']['user']['id_str']
                elif 'retweeted_status' in tweet.keys():
                    if 'quoted_status' in tweet['retweeted_status'].keys():
                        quoted_user_id_str = tweet['retweeted_status']['quoted_status']['user']['id_str']

            # Get Tweets
            tweet_data = {
                'tweet_id': tweet['id_str'],
                'user_id': tweet['user']['id_str'],
                'user_screen_name': tweet['user']['screen_name'],
                'full_text': text,
                'created_at': tweet['created_at'],
                'retweet_count': tweet['retweet_count'],
                'favorite_count': tweet['favorite_count'],
                'retweeted': tweet['retweeted'],
                'favorited': tweet['favorited'],
                'possibly_sensitive': None if 'possibly_sensitive' not in tweet.keys() else tweet['possibly_sensitive'],
                'truncated': tweet['truncated'],
                'in_reply_to_screen_name': tweet['in_reply_to_screen_name'],
                'in_reply_to_status_id_str': tweet['in_reply_to_status_id_str'],
                'in_reply_to_user_id_str': tweet['in_reply_to_user_id_str'],
                'is_quote_status': tweet['is_quote_status'],
                'quoted_status_id_str': None if 'quoted_status_id_str' not in tweet.keys() else tweet['quoted_status_id_str'],
                'quoted_user_id_str': quoted_user_id_str,
                'retweeted_status_id_str': None if 'retweeted_status' not in tweet.keys() else tweet['retweeted_status']['id_str'],
                'retweeted_user_id_str': None if 'retweeted_status' not in tweet.keys() else tweet['retweeted_status']['user']['id_str'],
                'media': None if 'extended_entities' not in tweet.keys() else self.get_media(tweet['extended_entities']['media']),
                'hashtags': False if len(tweet['entities']['hashtags']) == 0 else True,
                'lang': tweet['lang'],
                'place': None,
                'root_node': retweets_data[tw]['tweet_id'],
                'keyword': retweets_data[tw]['keyword'],
                'crawled_date': date_today,
                'crawled_time': time_today
            }

            user_data = {
                'user_id': user['id_str'],
                'created_at': user['created_at'],
                'total_tweets': user['statuses_count'],
                'followers_count': user['followers_count'],
                'favourites_count': user['favourites_count'],
                'friends_count': user['friends_count'],
                'listed_count': user['listed_count'],
                'following': user['following'],
                'location': user['location'],
                'protected': user['protected'],
                'screen_name': user['screen_name'],
                'verified': user['verified'],
                'crawled_at': crawl_time
            }

            self.db[self.db_index]['HO Nodes {}'.format(date_today)].insert_one(tweet_data)
            self.db[self.db_index]['Users {}'.format(date_today)].insert_one(user_data)
            self.db[self.db_index]['JSONs {}'.format(date_today)].insert_one(json_data)

        print('Uploaded {} retweets from health organizations!'.format(len(retweets)))

        retweets = []
        retweets_data = []
        self.get_HO_replies_and_quote_tweets(networks, stop)


    def get_HO_replies_and_quote_tweets(self, networks, stop):
        #############################################################################
        #                                                                           #
        # Getting the replies and quoted tweets from health-organizations-tweets    #
        # { list } networks - list of current health-organizations-networks         #
        # { Event } stop - event to stop threading                                  #
        #                                                                           #
        #############################################################################

        today = DT.datetime.now()
        date_today = '{}-{}-{}'.format(today.strftime("%Y"), today.strftime("%m"), today.strftime("%d"))
        time_today = '{}:{}:{}'.format(today.strftime("%H"), today.strftime("%M"), today.strftime("%S"))
        crawl_time = '{} {}'.format(date_today, time_today)

        tweet_threads = []
        replies = []
        quote_tweets = []
        new_networks = []
        threads_data = []
        idx = 0

        while idx < len(self.HO_tweets.index):
            # print('Count: {} | Index: {}'.format(count, idx))
            screen_name = self.HO_tweets.iloc[idx]['user_screen_name']
            tweet_id = int(self.HO_tweets.iloc[idx]['tweet_id'])
            keyword = self.HO_tweets.iloc[idx]['keyword']

            try:
                rate_limit = self.api[1].rate_limit_status()
                remaining = rate_limit['resources']['search']['/search/tweets']['remaining']

                if remaining > 5:
                    extracted = tweepy.Cursor(self.api[1].search, q='{}'.format(screen_name), since_id=tweet_id, tweet_mode='extended').items(500)
                    self.check_api_rate_limit(1, 'search', '/search/tweets')
                else:
                    extracted = tweepy.Cursor(self.api[2].search, q='{}'.format(screen_name), since_id=tweet_id, tweet_mode='extended').items(500)
                    self.check_api_rate_limit(2, 'search', '/search/tweets')

                tw = [e for e in extracted]
                print('Threads of [{}]: {} tweets'.format(tweet_id, len(tw)))
                if len(tw) > 0:
                    for i in range(len(tw)):
                        threads_data.append({
                            'tweet_id': str(tweet_id),
                            'screen_name': screen_name,
                            'full_text': self.HO_tweets.iloc[idx]['full_text'],
                            'keyword': keyword
                        })
                    tweet_threads.extend(tw)
                
                idx += 1
            except tweepy.TweepError as e:
                print(e)
                idx += 1
                continue

        for tw in range(len(tweet_threads)):
            tweet = tweet_threads[tw]._json
            user = tweet_threads[tw].user._json
            json_data = { 'tweet': tweet, 'user': user, 'root_node': threads_data[tw]['tweet_id'] }

            quoted_user_id_str = None
            if tweet['is_quote_status']:
                if 'quoted_status' in tweet.keys():
                    quoted_user_id_str = tweet['quoted_status']['user']['id_str']
                elif 'retweeted_status' in tweet.keys():
                    if 'quoted_status' in tweet['retweeted_status'].keys():
                        quoted_user_id_str = tweet['retweeted_status']['quoted_status']['user']['id_str']

            if 'retweeted_status' not in tweet.keys():
                # Get Tweets
                tweet_data = {
                    'tweet_id': tweet['id_str'],
                    'user_id': tweet['user']['id_str'],
                    'user_screen_name': tweet['user']['screen_name'],
                    'full_text': tweet['full_text'],
                    'created_at': tweet['created_at'],
                    'retweet_count': tweet['retweet_count'],
                    'favorite_count': tweet['favorite_count'],
                    'retweeted': tweet['retweeted'],
                    'favorited': tweet['favorited'],
                    'possibly_sensitive': None if 'possibly_sensitive' not in tweet.keys() else tweet['possibly_sensitive'],
                    'truncated': tweet['truncated'],
                    'in_reply_to_screen_name': tweet['in_reply_to_screen_name'],
                    'in_reply_to_status_id_str': tweet['in_reply_to_status_id_str'],
                    'in_reply_to_user_id_str': tweet['in_reply_to_user_id_str'],
                    'is_quote_status': tweet['is_quote_status'],
                    'quoted_status_id_str': None if 'quoted_status_id_str' not in tweet.keys() else tweet['quoted_status_id_str'],
                    'quoted_user_id_str': quoted_user_id_str,
                    'retweeted_status_id_str': None if 'retweeted_status' not in tweet.keys() else tweet['retweeted_status']['id_str'],
                    'retweeted_user_id_str': None if 'retweeted_status' not in tweet.keys() else tweet['retweeted_status']['user']['id_str'],
                    'media': None if 'extended_entities' not in tweet.keys() else self.get_media(tweet['extended_entities']['media']),
                    'hashtags': False if len(tweet['entities']['hashtags']) == 0 else True,
                    'lang': tweet['lang'],
                    'place': None,
                    'root_node': threads_data[tw]['tweet_id'],
                    'keyword': threads_data[tw]['keyword'],
                    'crawled_date': date_today,
                    'crawled_time': time_today
                }

                user_data = {
                    'user_id': user['id_str'],
                    'created_at': user['created_at'],
                    'total_tweets': user['statuses_count'],
                    'followers_count': user['followers_count'],
                    'favourites_count': user['favourites_count'],
                    'friends_count': user['friends_count'],
                    'listed_count': user['listed_count'],
                    'following': user['following'],
                    'location': user['location'],
                    'protected': user['protected'],
                    'screen_name': user['screen_name'],
                    'verified': user['verified'],
                    'crawled_at': crawl_time
                }

                # getting replies only
                if tweet_data['in_reply_to_status_id_str'] == threads_data[tw]['tweet_id']:
                    # print('{} | {}'.format(tweet_data['in_reply_to_status_id_str'], threads_data[tw]['tweet_id']))
                    new_networks.append({
                        'root_node': threads_data[tw]['tweet_id'],
                        'full_text': tweet_data['full_text'],
                        'tweet_id': tweet_data['tweet_id'],
                        'retweet_count': tweet_data['retweet_count'],
                        'screen_name': user_data['screen_name'],
                        'keyword': tweet_data['keyword'],
                        'root_date': date_today,
                        'date': date_today
                    })
                    self.db[self.db_index]['HO Nodes {}'.format(date_today)].insert_one(tweet_data)
                    self.db[self.db_index]['Users {}'.format(date_today)].insert_one(user_data)
                    self.db[self.db_index]['JSONs {}'.format(date_today)].insert_one(json_data)
                
                # getting quote tweets only
                if tweet_data['is_quote_status'] == True and tweet_data['quoted_status_id_str'] == threads_data[tw]['tweet_id']:
                    new_networks.append({
                        'root_node': threads_data[tw]['tweet_id'],
                        'full_text': tweet_data['full_text'],
                        'tweet_id': tweet_data['tweet_id'],
                        'retweet_count': tweet_data['retweet_count'],
                        'screen_name': user_data['screen_name'],
                        'keyword': tweet_data['keyword'],
                        'root_date': date_today,
                        'date': date_today
                    })
                    self.db[self.db_index]['HO Nodes {}'.format(date_today)].insert_one(tweet_data)
                    self.db[self.db_index]['Users {}'.format(date_today)].insert_one(user_data)
                    self.db[self.db_index]['JSONs {}'.format(date_today)].insert_one(json_data)

        # add the new networks to database
        count = 0
        for net in new_networks:
            if any(net['tweet_id'] == n['tweet_id'] for n in networks):
                self.main_db['health-organizations-networks'].update_one({ 'tweet_id': net['tweet_id'] }, { '$set': { 'retweet_count': net['retweet_count'] } })
            else:
                count += 1
                self.main_db['health-organizations-networks'].insert_one(net)

        print('New nodes from health organizations: {}'.format(count))
        print('Data uploaded!')

        tweet_threads = []
        replies = []
        quote_tweets = []
        new_networks = []
        threads_data = []

        stop.set()
        self.threads[0]._delete()