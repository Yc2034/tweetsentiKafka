from kafka import KafkaConsumer, KafkaClient
import json


# Simple Consumer Class
class Consumer():
    def __init__(self):
        self.kafka = KafkaClient("localhost:9092")
        self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                      auto_offset_reset='earliest',
                                      consumer_timeout_ms=10000)

    # Basic read data(doesn't do anything)
    def count_verify(self):
        self.consumer.subscribe(['tweets'])
        f = open("tweet_num_verified.csv","a")
        last_time = None
        count = 0
        for message in self.consumer:
            document = json.loads(message.value.decode("utf-8"))
            verify = document["user"]["verified"]
            #print(verify)
            time = document.get("created_at").split(':')[0]
            if time != last_time:
                if verify == True:
                    if last_time is not None:
                        f.write(last_time+','+str(count)+'\n')
                        print(last_time, count, sep=':')
                    count = 1
                    last_time = time
            else:
                if verify == True:
                    count += 1


if __name__ == '__main__':
    consumer = Consumer()
    consumer.count_verify()
