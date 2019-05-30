from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from pprint import pprint
import pandas as pd
from datetime import datetime
from dateutil import parser
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
        pprint(jsn)
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
                if ("купл", "продам", "приму в дар", "объявление") not in tmp:
                    text = tmp['extended_tweet']['full_text']

        # if (("строен","cроительст","ремонт","реконструкц")&("больниц","поликлин","госпиталь","травмпункт", "медучреждение") or ("повышен", "увелич","поднят") & ("ЗП", "зароботная плата" , "зароботной платы" , "зароботной плате" , "зароботной платой") & ("врач"  , "медработник"   , "медсестр")  or ("спонсирован"  , "выделено"  , "будет выделен"  , "проспонсирован" ) & ("исследован"  , "технолог"  , "разработк"  , "") or ("увеличен"  , "повышен"  , "изменен")&("пенсионный возраст"  , "пенсион") or ("организац"  , "организован"  , "создан")&("секци"  , "молодежн"  , "организац"  , "кружок"  , "молодежн клуб"  ) or ("введен"  , "установлен"  , "улучшен"  , "повышен"  , "увеличен" )&("пенси"  , "льгот"  , "дотац"  , "скидк") or ("организов"  , "планирует"  , "создан")&("форум"  , "конференц"  , "лагер"  , "хакатон"  , "мероприят")&("молодеж"  , "юношеск"  , "молод" ) or ("изменен"  , "введен"  , "увелич"  , "уменьшен"  , "проиндексир")&("налог"  , "цен")&("физ" , "юр")&("предприним"  , "бизнесмен"  , "лиц"  , "чиновник") or ("введен"  , "изменен" , "увелич"  , "уменьшен")&("сбор"  , "курорт")) in text:
            ht = ''
        for hashtag in jsn['entities']['hashtags']:
            ht = hashtag['text']
            print(hashtag['text'])
            # Date                            " + "GEO" + "    " + "HASHTAGS" + " " + "USER ID" + " " + "TEXT")
            date = jsn['created_at']
            # Geo
            geo = (jsn['geo'] if jsn['geo'] is not None else 'None')
            # id_users
            idut = str(jsn['user']['id'])
            # print(jsn['created_at'] + " " + (jsn['geo'] if jsn['geo'] is not None else 'None') + "  " + ht + "  " + str(jsn['user']['id']) + " " + text)
            print('\n')
            # year = datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p')
            date = parser.parse(date)
            year = str(date.year)
            month = str(date.month)
            day = str(date.day)
            print(day, month, year)
            print('\n')
            print('\n')
            print(geo)
            print(text)
            print(ht)
            print(idut)
            print('\n')
            print('\n')
            # Id_user
            idu = jsn['user']['id_str']
            # Geolocation of user
            geous = str(jsn['user']['location'] if jsn['user']['location'] is not None else 'None')
            # User name
            uname = jsn['user']['name']
            # User login
            logname = jsn['user']['screen_name']
            print(idu + " " + geous + " " + uname + " " + logname)
            post = pd.DataFrame(columns=['idut', 'day', 'month', 'year', 'geo', 'text', 'ht'])
            post.loc[0] = [idut, day, month, year,geo, text, ht]
            user = pd.DataFrame(columns=['idut', 'geous', 'uname', 'logname'])
            user.loc[0] = [idut, geous, uname, logname]
            project_id = 'arctic-operand-238808'
            private_key = 'arctic-operand-238808-f3da5a210f89.json'
            post.to_gbq('tweetpost.post', project_id=project_id, if_exists='append', private_key=private_key)
            user.to_gbq('tweetpost.user', project_id=project_id, if_exists='append', private_key=private_key)
            print('\n')
            print('\n')
            print(type(geous))
            print("!!! NEXT TWEET !!!")

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
        time.sleep(10)
        return True


if __name__ == "__main__":
    i = 0
    for tweet in tweepy.Cursor(api.search,q="Горячий Ключ",lang ="ru").items():
        text = tweet.text
        jsn = tweet._json
        date = jsn['created_at']
        date = parser.parse(date)
        year = str(date.year)
        month = str(date.month)
        day = str(date.day)
        idu = jsn['user']['id_str']
        idut = str(jsn['user']['id'])
        geo = (jsn['geo'] if jsn['geo'] is not None else 'None')
        geous = str(jsn['user']['location'] if jsn['user']['location'] is not None else 'None')
        uname = jsn['user']['name']
        logname = jsn['user']['screen_name']
        ht = ''
        for hashtag in jsn['entities']['hashtags']:
            ht = hashtag['text']
        if 'extended_tweet' in jsn:
            text = jsn['extended_tweet']['full_text']
        if 'retweeted_status' in jsn:
            # print("in r status")
            tmp = jsn['retweeted_status']
            # pprint(tmp)
            if 'extended_tweet' in tmp:
                if ("купл", "продам", "продаж",  "приму в дар", "объявление") not in tmp:
                    text = tmp['extended_tweet']['full_text']
                else:
                    continue
        post = pd.DataFrame(columns=['idut', 'day', 'month', 'year', 'geo', 'text', 'ht'])
        post.loc[0] = [idut, day, month, year, geo, text, ht]
        user = pd.DataFrame(columns=['idut', 'geous', 'uname', 'logname'])
        user.loc[0] = [idut, geous, uname, logname]
        project_id = 'arctic-operand-238808'
        private_key = 'arctic-operand-238808-f3da5a210f89.json'
        post.to_gbq('tweetpost.post', project_id=project_id, if_exists='append', private_key=private_key)
        user.to_gbq('tweetpost.user', project_id=project_id, if_exists='append', private_key=private_key)
        print(i)
        ++i
        # pprint("login: " + logname + " name: " + uname + " user location: " + geous + " User_ID: " + idu)
        # pprint("User_ID: " + idu + " day: " + day + " month: " + month + " year: " + year + " geolocation: " + geo + " text: " + text + " hashtags: " +ht)

    stream_listener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener, tweet_mode="extended")
    # stream.filter(locations=[38.0, 44.0, 40,46], track=["Краснодар", "Краснодарский край", "сочи", "Анапа", "Апшеронск", "Армавир", "Белореченск", "Геленджик", "Горячий Ключ", "Гулькевич", "Ейск", "Кореновск", "Краснодар", "Кропоткин", "Крымск", "Курганинск", "Лабинск", "Новокубанск", "Новороссийск", "Приморско-Ахтарск", "Славянск-на-Кубани", "Сочи", "Темрюк", "Тимашёвск", "Тихорецк", "Туапсе", "Усть-Лабинск", "Хадыженск", "Кубан"], languages=["ru"])
    # stream.filter(track=['москва', 'краснодарский край', 'кубань', 'путин'], languages=["ru"])





