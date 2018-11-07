from kafka import KafkaConsumer, KafkaClient
import json
import sys
import csv
import time

from datetime import timedelta, datetime
from collections import Counter
from aylienapiclient import textapi
from aylienapiclient.errors import HttpError

application_key = ''
application_id = ''

text_client = textapi.Client(application_id, application_key)

#Use this consumer to answer medium difficulty questions
class Consumer():
    def __init__(self):
        self.kafka = KafkaClient("localhost:9092")
        self.tweet_consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                        auto_offset_reset='earliest',
                                        consumer_timeout_ms=10000)

        self.tweet_consumer.subscribe(['tweets'])
        self.weather_consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                            auto_offset_reset='earliest',
                                            consumer_timeout_ms=1000)
        self.weather_consumer.subscribe(['weather'])
        self.curr_weather = ""

        #Used to keep track of percentages
        self.count = { 'neutral' : 0, 'negative' : 0, 'positive': 0}
        self.percentages = { 'neutral' : 0, 'negative': 0, 'positive': 0}
        self.total = 0

    def read_data(self):
        self.tweet_consumer.subscribe(['tweets'])
        for message in self.tweet_consumer:
            #Extract text from tweet document
            document = json.loads(message.value.decode("utf-8"))
            if 'text' in document.keys():
                tidy_tweet = document['text'].strip().encode('ascii', 'ignore')
                try:
                    response = text_client.Sentiment({'text': str(tidy_tweet)})
                    self.total += 1
                    self.count[response['polarity']] += 1

                    for key in self.count.keys():
                        self.percentages[key] = self.count[key] / self.total
                except HttpError as err:
                                    if err.resp.status == 429:
                        time.sleep(5)


            weather_data = document['weather'][0]
            weather_str = str(weather_data)
            weather_str = weather_str.replace("'", '"')

            weather_dict = json.loads(weather_str)

            main_weather = weather_dict['main']
            self.percentages['weather'] = main_weather
            self.percentages['timestamp'] = datetime.now()

            document = self.percentages

    def write_data(self):
        write_dict = self.percentages
        write_dict['count'] = self.total
        write_dict['weather'] = self.curr_weather

        csv_writer.writerow(row)
        document = json.loads(write_dict)
        try:
            post_id = self.collection.insert_one(document)
        except BaseException as e:
            print("Could not insert into collection: %s" % str(e))

    def get_weather(self):
        return next(self.weather_consumer)

if __name__=='__main__':
    consumer = Consumer()
    consumer.read_data()
