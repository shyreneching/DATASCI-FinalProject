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

class TweetCrawlerV2:
    def __init__(self):
        self.credentials = None
        f = open('credentials.json',)
        self.credentials = json.load(f)
        self.HO = None
        self.main_db = None
        self.db_index = None
        self.db = []
        self.api = []
        self.toptweets = []
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

        auth = OAuthHandler(self.credentials['CONSUMER_KEY_IV'], self.credentials['CONSUMER_SECRET_IV'])
        auth.set_access_token(self.credentials['ACCESS_KEY_IV'], self.credentials['ACCESS_SECRET_IV'])
        api4 = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

        auth = OAuthHandler(self.credentials['CONSUMER_KEY_V'], self.credentials['CONSUMER_SECRET_V'])
        auth.set_access_token(self.credentials['ACCESS_KEY_V'], self.credentials['ACCESS_SECRET_V'])
        api5 = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

        auth = OAuthHandler(self.credentials['CONSUMER_KEY_VI'], self.credentials['CONSUMER_SECRET_VI'])
        auth.set_access_token(self.credentials['ACCESS_KEY_VI'], self.credentials['ACCESS_SECRET_VI'])
        api6 = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

        self.api = [api1, api2, api3, api4, api5, api6]


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


    def scrap_tweets_thread(self):
        #############################################################################
        #                                                                           #
        # Setting up thread for scraping tweets.                                    #
        #                                                                           #
        #############################################################################

        self.setup_database()

        stop_event = threading.Event()
        t = threading.Thread(target=self.get_network, args=(stop_event,), daemon=True)
        self.threads.append(t)
        self.threads[-1].start()


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


    def get_network(self, stop):
        #############################################################################
        #                                                                           #
        # Getting the retweets from the tweets in the list.                         #
        # { Event } stop - event to stop threading                                  #
        #                                                                           #
        #############################################################################

        # get the current list of networks in database
        networks = list(self.main_db['networks'].find({}, { '_id': 0 }))
        HO_networks = list(self.main_db['health-organizations-networks'].find({}, { '_id': 0 }))

        # check and remove 2 days old tweets
        for net in networks:
            net_date = net['date'].split('-')
            net_date = date(int(net_date[0]), int(net_date[1]), int(net_date[2]))
            diff = date.today() - net_date
            if diff.days > 1:
                self.main_db['networks'].delete_one({ 'tweet_id': net['tweet_id'] })

        for net in HO_networks:
            net_date = net['date'].split('-')
            net_date = date(int(net_date[0]), int(net_date[1]), int(net_date[2]))
            diff = date.today() - net_date
            if diff.days > 1:
                self.main_db['health-organizations-networks'].delete_one({ 'tweet_id': net['tweet_id'] })

        networks = list(self.main_db['networks'].find({}, { '_id': 0 }))
        HO_networks = list(self.main_db['health-organizations-networks'].find({}, { '_id': 0 }))
        print('Alive Nodes: {}'.format(len(networks) + len(HO_networks)))

        self.get_retweets(networks, HO_networks, stop)


    def get_retweets(self, networks, HO_networks, stop):
        #############################################################################
        #                                                                           #
        # Getting the retweets from the tweets in the top tweets.                   #
        # { list } networks - list of current network in the database               #
        # { list } HO_networks - list of current health-organizations-networks      #
        # { Event } stop - event to stop threading                                  #
        #                                                                           #
        #############################################################################

        today = DT.datetime.now()
        date_today = '{}-{}-{}'.format(today.strftime("%Y"), today.strftime("%m"), today.strftime("%d"))
        time_today = '{}:{}:{}'.format(today.strftime("%H"), today.strftime("%M"), today.strftime("%S"))
        crawl_time = '{} {}'.format(date_today, time_today)

        retweets = []
        retweets_data = []

        for idx in range(len(networks)):
            retweet_count = networks[idx]['retweet_count']

            if retweet_count > 0:
                # print('Count: {} | Index: {}'.format(count, idx))
                tweet_id = int(networks[idx]['tweet_id'])
                screen_name = networks[idx]['screen_name']

                try:
                    rt = self.api[0].retweets(tweet_id, retweet_count)
                    self.check_api_rate_limit(0, 'statuses', '/statuses/retweets/:id')

                    print('Retweets of [{}]: {} of {} | {:.2f}%'.format(tweet_id, len(rt), retweet_count, len(rt)*100/retweet_count))
                    if len(rt) > 0:
                        for i in range(len(rt)):
                            retweets_data.append({
                                'full_text': networks[idx]['full_text'],
                                'tweet_id': networks[idx]['tweet_id'],
                                'root_node': networks[idx]['root_node'],
                                'screen_name': networks[idx]['screen_name'],
                                'date': networks[idx]['root_date'],
                                'keyword': networks[idx]['keyword']
                            })
                        retweets.extend(rt)

                except tweepy.TweepError as e:
                    print(e)
                    continue
        
        print('Total retweets: {}'.format(len(retweets)))

        for tw in range(len(retweets)):
            tweet = retweets[tw]._json
            user = retweets[tw].user._json
            json_data = { 'tweet': tweet, 'user': user, 'root_node': retweets_data[tw]['root_node'] }

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
                'root_node': retweets_data[tw]['root_node'],
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

            self.db[self.db_index]['Nodes {}'.format(retweets_data[tw]['date'])].insert_one(tweet_data)
            self.db[self.db_index]['Users {}'.format(retweets_data[tw]['date'])].insert_one(user_data)
            self.db[self.db_index]['JSONs {}'.format(retweets_data[tw]['date'])].insert_one(json_data)

        print('Uploaded {} retweets!'.format(len(retweets)))

        retweets = []
        retweets_data = []
        self.get_replies_and_quote_tweets(networks, HO_networks, stop)
    

    def get_replies_and_quote_tweets(self, networks, HO_networks, stop):
        #############################################################################
        #                                                                           #
        # Getting the replies and quoted tweets from the tweets in the top tweets.  #
        # { list } networks - list of current network in the database               #
        # { list } HO_networks - list of current health-organizations-networks      #
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

        for idx in range(len(networks)):
            # print('Count: {} | Index: {}'.format(count, idx))
            screen_name = networks[idx]['screen_name']
            tweet_id = int(networks[idx]['tweet_id'])

            try:
                u = self.api[0].get_user(screen_name)
                u = u._json

                if not u['protected']:
                    # rate_limit_1 = self.api[1].rate_limit_status()
                    # remaining_1 = rate_limit_1['resources']['search']['/search/tweets']['remaining']
                    # rate_limit_2 = self.api[2].rate_limit_status()
                    # remaining_2 = rate_limit_2['resources']['search']['/search/tweets']['remaining']      

                    # if remaining_1 > 5:
                    #     extracted = tweepy.Cursor(self.api[1].search, q='{}'.format(screen_name), since_id=tweet_id, tweet_mode='extended').items(500)
                    #     rate_limit = self.api[1].rate_limit_status()
                    #     limit = rate_limit['resources']['search']['/search/tweets']['limit']
                    #     remaining = rate_limit['resources']['search']['/search/tweets']['remaining']
                    #     print('API 2 [/search/tweets] - Requests remaining: {} out of {}'.format(remaining, limit))
                    # elif remaining_2 > 5:
                    #     extracted = tweepy.Cursor(self.api[2].search, q='{}'.format(screen_name), since_id=tweet_id, tweet_mode='extended').items(500)
                    #     rate_limit = self.api[2].rate_limit_status()
                    #     limit = rate_limit['resources']['search']['/search/tweets']['limit']
                    #     remaining = rate_limit['resources']['search']['/search/tweets']['remaining']
                    #     print('API 3 [/search/tweets] - Requests remaining: {} out of {}'.format(remaining, limit))
                    # else:
                    extracted = tweepy.Cursor(self.api[3].search, q='{}'.format(screen_name), since_id=tweet_id, tweet_mode='extended').items(500)
                    self.check_api_rate_limit(3, 'search', '/search/tweets')
                    
                    tw = [e for e in extracted]

                    print('Threads of [{}]: {} tweets'.format(tweet_id, len(tw)))
                    if len(tw) > 0:
                        for i in range(len(tw)):
                            threads_data.append({
                                'tweet_id': str(tweet_id),
                                'screen_name': screen_name,
                                'full_text': networks[idx]['full_text'],
                                'root_node': networks[idx]['root_node'],
                                'date': networks[idx]['root_date'],
                                'keyword': networks[idx]['keyword']
                            })
                        tweet_threads.extend(tw)
                
            except tweepy.TweepError as e:
                print(e)
                continue

        for tw in range(len(tweet_threads)):
            tweet = tweet_threads[tw]._json
            user = tweet_threads[tw].user._json
            json_data = { 'tweet': tweet, 'user': user, 'root_node': threads_data[tw]['root_node'] }

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
                    'root_node': threads_data[tw]['root_node'],
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
                        'root_node': threads_data[tw]['root_node'],
                        'full_text': tweet_data['full_text'],
                        'tweet_id': tweet_data['tweet_id'],
                        'retweet_count': tweet_data['retweet_count'],
                        'screen_name': user_data['screen_name'],
                        'keyword': tweet_data['keyword'],
                        'root_date': threads_data[tw]['date'],
                        'date': date_today
                    })
                    self.db[self.db_index]['Nodes {}'.format(threads_data[tw]['date'])].insert_one(tweet_data)
                    self.db[self.db_index]['Users {}'.format(threads_data[tw]['date'])].insert_one(user_data)
                    self.db[self.db_index]['JSONs {}'.format(threads_data[tw]['date'])].insert_one(json_data)
                
                # getting quote tweets only
                if tweet_data['is_quote_status'] == True and tweet_data['quoted_status_id_str'] == threads_data[tw]['tweet_id']:
                    new_networks.append({
                        'root_node': threads_data[tw]['root_node'],
                        'full_text': tweet_data['full_text'],
                        'tweet_id': tweet_data['tweet_id'],
                        'retweet_count': tweet_data['retweet_count'],
                        'screen_name': user_data['screen_name'],
                        'keyword': tweet_data['keyword'],
                        'root_date': threads_data[tw]['date'],
                        'date': date_today
                    })
                    self.db[self.db_index]['Nodes {}'.format(threads_data[tw]['date'])].insert_one(tweet_data)
                    self.db[self.db_index]['Users {}'.format(threads_data[tw]['date'])].insert_one(user_data)
                    self.db[self.db_index]['JSONs {}'.format(threads_data[tw]['date'])].insert_one(json_data)

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
        self.get_HO_retweets(HO_networks, stop)


    def get_HO_retweets(self, HO_networks, stop):
        #############################################################################
        #                                                                           #
        # Getting the retweets from the tweets in the health-organizations-tweets   #
        # { list } HO_networks - list of current health-organizations-networks      #
        # { Event } stop - event to stop threading                                  #
        #                                                                           #
        #############################################################################

        today = DT.datetime.now()
        date_today = '{}-{}-{}'.format(today.strftime("%Y"), today.strftime("%m"), today.strftime("%d"))
        time_today = '{}:{}:{}'.format(today.strftime("%H"), today.strftime("%M"), today.strftime("%S"))
        crawl_time = '{} {}'.format(date_today, time_today)

        retweets = []
        retweets_data = []

        for idx in range(len(HO_networks)):
            retweet_count = HO_networks[idx]['retweet_count']

            if retweet_count > 0:
                # print('Count: {} | Index: {}'.format(count, idx))
                tweet_id = int(HO_networks[idx]['tweet_id'])

                try:
                    rt = self.api[4].retweets(tweet_id, retweet_count)
                    self.check_api_rate_limit(4, 'statuses', '/statuses/retweets/:id')

                    print('Retweets of [{}]: {} of {} | {:.2f}%'.format(tweet_id, len(rt), retweet_count, len(rt)*100/retweet_count))
                    if len(rt) > 0:
                        for i in range(len(rt)):
                            retweets_data.append({
                                'full_text': HO_networks[idx]['full_text'],
                                'tweet_id': HO_networks[idx]['tweet_id'],
                                'root_node': HO_networks[idx]['root_node'],
                                'screen_name': HO_networks[idx]['screen_name'],
                                'date': HO_networks[idx]['root_date'],
                                'keyword': HO_networks[idx]['keyword']
                            })
                        retweets.extend(rt)                        
                    
                except tweepy.TweepError as e:
                    print(e)
                    continue
        
        print('Total retweets from health organizations: {}'.format(len(retweets)))

        for tw in range(len(retweets)):
            tweet = retweets[tw]._json
            user = retweets[tw].user._json
            json_data = { 'tweet': tweet, 'user': user, 'root_node': retweets_data[tw]['root_node'] }

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
                'root_node': retweets_data[tw]['root_node'],
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

            self.db[self.db_index]['HO Nodes {}'.format(retweets_data[tw]['date'])].insert_one(tweet_data)
            self.db[self.db_index]['Users {}'.format(retweets_data[tw]['date'])].insert_one(user_data)
            self.db[self.db_index]['JSONs {}'.format(retweets_data[tw]['date'])].insert_one(json_data)

        print('Uploaded {} retweets from health organizations!'.format(len(retweets)))

        retweets = []
        retweets_data = []
        self.get_HO_replies_and_quote_tweets(HO_networks, stop)


    def get_HO_replies_and_quote_tweets(self, HO_networks, stop):
        #############################################################################
        #                                                                           #
        # Getting the replies and quoted tweets from health-organizations-tweets    #
        # { list } HO_networks - list of current health-organizations-networks      #
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

        for idx in range(len(HO_networks)):
            # print('Count: {} | Index: {}'.format(count, idx))
            screen_name = HO_networks[idx]['screen_name']
            tweet_id = int(HO_networks[idx]['tweet_id'])

            try:
                u = self.api[0].get_user(screen_name)
                u = u._json

                if not u['protected']:
                    rate_limit_1 = self.api[4].rate_limit_status()
                    remaining_1 = rate_limit_1['resources']['search']['/search/tweets']['remaining']
                    rate_limit_2 = self.api[0].rate_limit_status()
                    remaining_2 = rate_limit_2['resources']['search']['/search/tweets']['remaining']

                    if remaining_1 > 5:
                        extracted = tweepy.Cursor(self.api[4].search, q='{}'.format(screen_name), since_id=tweet_id, tweet_mode='extended').items(500)
                        self.check_api_rate_limit(4, 'search', '/search/tweets')
                    # elif remaining_2 > 5:
                    else:
                        extracted = tweepy.Cursor(self.api[0].search, q='{}'.format(screen_name), since_id=tweet_id, tweet_mode='extended').items(500)
                        self.check_api_rate_limit(0, 'search', '/search/tweets')
                    # else:
                    #     extracted = tweepy.Cursor(self.api[5].search, q='{}'.format(screen_name), since_id=tweet_id, tweet_mode='extended').items(500)
                    #     rate_limit = self.api[5].rate_limit_status()
                    #     limit = rate_limit['resources']['search']['/search/tweets']['limit']
                    #     remaining = rate_limit['resources']['search']['/search/tweets']['remaining']
                    #     print('API 6 [/search/tweets] - Requests remaining: {} out of {}'.format(remaining, limit))
                    
                    tw = [e for e in extracted]
                    print('Threads of [{}]: {} tweets'.format(tweet_id, len(tw)))
                    if len(tw) > 0:
                        for i in range(len(tw)):
                            threads_data.append({
                                'tweet_id': str(tweet_id),
                                'screen_name': screen_name,
                                'full_text': HO_networks[idx]['full_text'],
                                'root_node': HO_networks[idx]['root_node'],
                                'date': HO_networks[idx]['root_date'],
                                'keyword': HO_networks[idx]['keyword']
                            })
                        tweet_threads.extend(tw)
                
            except tweepy.TweepError as e:
                print(e)
                continue

        for tw in range(len(tweet_threads)):
            tweet = tweet_threads[tw]._json
            user = tweet_threads[tw].user._json
            json_data = { 'tweet': tweet, 'user': user, 'root_node': threads_data[tw]['root_node'] }

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
                    'root_node': threads_data[tw]['root_node'],
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
                        'root_node': threads_data[tw]['root_node'],
                        'full_text': tweet_data['full_text'],
                        'tweet_id': tweet_data['tweet_id'],
                        'retweet_count': tweet_data['retweet_count'],
                        'screen_name': user_data['screen_name'],
                        'keyword': tweet_data['keyword'],
                        'root_date': threads_data[tw]['date'],
                        'date': date_today
                    })
                    self.db[self.db_index]['HO Nodes {}'.format(threads_data[tw]['date'])].insert_one(tweet_data)
                    self.db[self.db_index]['Users {}'.format(threads_data[tw]['date'])].insert_one(user_data)
                    self.db[self.db_index]['JSONs {}'.format(threads_data[tw]['date'])].insert_one(json_data)
                
                # getting quote tweets only
                if tweet_data['is_quote_status'] == True and tweet_data['quoted_status_id_str'] == threads_data[tw]['tweet_id']:
                    new_networks.append({
                        'root_node': threads_data[tw]['root_node'],
                        'full_text': tweet_data['full_text'],
                        'tweet_id': tweet_data['tweet_id'],
                        'retweet_count': tweet_data['retweet_count'],
                        'screen_name': user_data['screen_name'],
                        'keyword': tweet_data['keyword'],
                        'root_date': threads_data[tw]['date'],
                        'date': date_today
                    })
                    self.db[self.db_index]['HO Nodes {}'.format(threads_data[tw]['date'])].insert_one(tweet_data)
                    self.db[self.db_index]['Users {}'.format(threads_data[tw]['date'])].insert_one(user_data)
                    self.db[self.db_index]['JSONs {}'.format(threads_data[tw]['date'])].insert_one(json_data)

        # add the new networks to database
        count = 0
        for net in new_networks:
            if any(net['tweet_id'] == n['tweet_id'] for n in HO_networks):
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