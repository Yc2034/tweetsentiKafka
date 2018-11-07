from kafka import KafkaConsumer, KafkaClient
from pymongo import MongoClient
import json

class TweetConsumer():
    def __init__(self):
        self.kafka = KafkaClient("localhost:9092")
        self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                      auto_offset_reset='earliest',
                                      consumer_timeout_ms=10000)

        self.mongo_client = MongoClient('localhost', 27017)
        self.db = self.mongo_client.nosql
        self.collection = self.db.nyctweets

    def read_data(self):
        self.consumer.subscribe(['tweets'])
        for message in self.consumer:
            document = json.loads(message.value.decode("utf-8"))
            print(document)
            try:
                post_id = self.collection.insert_one(document)
            except BaseException as e:
                print("Could not insert into collection: %s" % str(e))


if __name__=='__main__':
    tweet_consumer = TweetConsumer()
    tweet_consumer.read_data()
