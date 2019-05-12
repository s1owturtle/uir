from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from pprint import pprint
import pandas as pd
# from google.oauth2 import service_account
import time
# Import the necessary package to process data in JSON format
try:
    import json
except ImportError:
    import simplejson as json

import tweepy

ACCESS_TOKEN = '1116667192110669824-xHeFa1Du1m8TiRkb3Q40LZAAr5WgIT'
ACCESS_SECRET = 'AHuW5rsw5yXXXiQuqL1YDWg2pmq8pf1crnAJjPLtskzRL'
CONSUMER_KEY = '8nQYEOClgrE1I0ZtCMCwjrzuH'
CONSUMER_SECRET = 'tcRF9W1VChTjJ2Cu6MzuY8hJNEa3UMAZ94CCsakGovtU17ZtLg'

# Setup tweepy to authenticate with Twitter credentials:

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, compression=True)


from tweepy.utils import import_simplejson
json = import_simplejson()


class StreamListener(tweepy.StreamListener):

    def on_status(self, status):
        #print(status.text)

        jsn = status._json
        # pprint(jsn)
        text = status.text
        table1 = status.text
        table2 = status.text
        if 'extended_tweet' in jsn:
            text = jsn['extended_tweet']['full_text']
        if 'retweeted_status' in jsn:
            # print("in r status")
            tmp = jsn['retweeted_status']
            # pprint(tmp)
            if 'extended_tweet' in tmp:
                text = tmp['extended_tweet']['full_text']

        ht = ''
        for hashtag in jsn['entities']['hashtags']:
            ht = hashtag['text']
            print(hashtag['text'])

        print("DATE" + "                           " + "GEO" + "    " + "HASHTAGS" + " " + "USER ID" + " " + "TEXT")
        print(jsn['created_at'] + " " + (jsn['geo'] if jsn['geo'] is not None else 'None') + "  " + ht + "  " + str(jsn['user']['id']) + " " + text)
        print('\n')
        print(jsn['created_at'])
        print(jsn['geo'])
        print(text)
        print(ht)
        print(str(jsn['user']['id']))
        print('\n')
        print('\n')
        print("USER ID    " + " " + "LOCATION               " + " " + "NAME  " + " " + "LOGIN")
        print(jsn['user']['id_str'] + " " + (jsn['user']['location'] if jsn['user']['location'] is not None else 'None') + " " + jsn['user']['name'] + " " + jsn['user']['screen_name'])
        print('\n')
        print('\n')
        print("!!! NEXT TWEET !!!")
        # if len(jsn['entities']['hashtags']) > 0:
        #     print(jsn['entities']['hashtags']['text'])

    # def on_data(self, raw_data):
    #     data = json.loads(raw_data)
    #     'retweeted_status
    #     print(data)
    #     return True/

    def on_error(self, status_code):
        if status_code == 420:
            print("Oops")
            return False

    def on_limit(self, status):
        print("Sleep 300 seconds")
        time.sleep(300)
        return True


if __name__ == "__main__":

    stream_listener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener, tweet_mode="extended")
    stream.filter(track=["Абинск, Анапа, Апшеронск, Армавир, Белореченск, Геленджик, Горячий Ключ, Гулькевич, Ейск, Кореновск,"
                         " Краснодар, Кропоткин, Крымск, Курганинск, Лабинск, Новокубанск, Новороссийск, Приморско-Ахтарск, Славянск-на-Кубани,"
                         " Сочи, Темрюк, Тимашёвск, Тихорецк, Туапсе, Усть-Лабинск, Хадыженск, Кубан"], languages=["ru"])





