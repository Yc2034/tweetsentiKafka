
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
WEATHER_KEY = '70d7f50107cf5cb5779718c1e81bf5ff6c001a11'
CONSUMER_KEY = '8Z8Pfu48KKDulVE8AOM4qziOr'
CONSUMER_SECRET = 'S5RX476EyLzUJFp6j8byz5UhqVfutj8ZNys0yJ23NkTTZ8dKUT'
ACCESS_TOKEN = '958762137085792256-xSChs1JaaJLIY49xjLB93Y2Ye7bzUdI'
ACCESS_SECRET = 'rODcKrY35Lc40xurlEGCwW4gtEr0v1x2Sd06JzQtdmCd9'
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


    def on_data(self, data):
        try:
            curr_tweet = json.loads(data)

            data = json.dumps(curr_tweet)
            self.producer.send_messages('tweets', data.encode('utf-8'))
            print("Successfully sent message to kafka")
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

    def on_error(self, status):
        print(status)

if __name__=='__main__':
    twitter_streamer = TweetStreamer()
    twitter_streamer.stream_tweets()
