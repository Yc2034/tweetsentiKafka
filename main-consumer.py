from kafka import KafkaConsumer, KafkaClient
import json

#Simple Consumer Class
class Consumer():
    def __init__(self):
        self.kafka = KafkaClient("localhost:9092")
        self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                      auto_offset_reset='earliest',
                                      consumer_timeout_ms=10000)

    # Basic read data(doesn't do anything)
    def read_data(self):
        self.consumer.subscribe(['tweets'])
        for message in self.consumer:
            document = json.loads(message.value.decode("utf-8"))

if __name__=='__main__':
    consumer = Consumer()
    consumer.read_data()
