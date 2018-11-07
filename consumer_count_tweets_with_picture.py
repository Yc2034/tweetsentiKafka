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
    def tweets_with_pic(self):
        self.consumer.subscribe(['tweets'])
        f=open("tweets_num_with_pictures.csv","a")
        last_time = None
        count = 0
        for message in self.consumer:
            document = json.loads(message.value.decode("utf-8"))
            #media = document.get("entities").get("media")
            print(document)
            time = document.get("created_at").split(':')[0]
            if time != last_time:
                if document.get("entities") is not None and  document.get("entities").get("media") is not None:
                    if last_time is not None:
                        f.write(last_time+','+str(count)+'\n')
                        print(last_time,count, sep=':')
                    count = 1
                    last_time = time
            else:
                if document.get("entities") is not None and  document.get("entities").get("media") is not None:
                    count+=1

    def read_weather(self):
        self.consumer.subsctibe(['weather'])
        for message in self.consumer:
            document = jason.loads(message.value.decode("utf-8"))
            print(document)

if __name__=='__main__':
    consumer = Consumer()
    consumer.tweets_with_pic()
    #consumer.read_weather()
