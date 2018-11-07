from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaClient, SimpleProducer
import json
import requests
import datetime
import time
import schedule
from collections import Counter
WEATHER_KEY = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''
ACCESS_TOKEN = ''
ACCESS_SECRET = ''

class TweetStreamer():
    def __init__(self):
        pass
    def stream_tweets(self):
        listener = StdOutListener()
        auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
        stream = Stream(auth, listener)
        stream.filter(locations=[-74.1687,40.5722,-73.8062,40.9467])

class StdOutListener(StreamListener):
    def __init__(self):
        self.kafka = KafkaClient("localhost:9092")
        self.producer = SimpleProducer(self.kafka)

        self.api_url = 'http://api.openweathermap.org/data/2.5/weather/'

    def get_curr_weather(self):
        self.params = {'q': 'new york', 'appid': WEATHER_KEY}
        r = requests.get(url = self.api_url, params = self.params)
        data = r.json()

        return data

    def on_data(self, data):
        try:
            curr_weather = self.get_curr_weather()
            curr_tweet = json.loads(data)
            message = {}
            message.update(curr_weather)
            message.update(curr_tweet)
            data = json.dumps(message)
            self.producer.send_messages('tweet-weather', data.encode('utf-8'))
            print("Successfully sent message to kafka")
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True
    def on_error(self, status):
        print(status)

if __name__=='__main__':
    twitter_streamer = TweetStreamer()
    twitter_streamer.stream_tweets()
