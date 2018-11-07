import json
import requests
from kafka import KafkaClient, SimpleProducer
import schedule
import datetime
import time
# Get the API Key
API_KEY = '3a10b1fd45007d0b7e373a4515216ad8'
class WeatherProducer():
    def __init__(self):
        self.api_url = 'http://api.openweathermap.org/data/2.5/weather/'
        self.kafka = KafkaClient("localhost:9092")
        self.producer = SimpleProducer(self.kafka)

    def get_curr_weather(self):
        self.params = {'q': 'new york', 'appid': API_KEY}
        r = requests.get(url = self.api_url, params = self.params)
        data = r.json()

        timestamp = datetime.datetime.now()
        data['timestamp'] = timestamp

        return data

    def myconverter(self, o):
        if isinstance(o, datetime.datetime):
            return o.__str__()
    def send_to_kafka(self):
        data = self.get_curr_weather()
        try:
            self.producer.send_messages('weather', json.dumps(data, default=self.myconverter).encode('utf-8'))
            print("Successfully sent to kafka")
        except BaseException as e:
            print("Error on_data %s" % str(e))
def job():
    weather_producer = WeatherProducer()
    weather_producer.send_to_kafka()
if __name__=='__main__':
    schedule.every(1).minutes.do(job)
    while 1:
        schedule.run_pending()
        time.sleep(1)
