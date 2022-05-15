import time
import json
import requests
import datetime
from kafka import KafkaProducer, KafkaClient
from websocket import create_connection

def get_computer_data(computer_id):
    
    try:
        url = 'http://0.0.0.0:3030/computer_data/{0}'.format(computer_id)
        r = requests.get(url)
        return r.text
    except:
        return "Error in Connection"


producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
computers_number=3


while True:
    time.sleep(1)
    for id in range(computers_number):
        msg =  get_computer_data(id)
        producer.send("RawcomputerData", msg.encode('utf-8'))
        


    

