#!/usr/bin/env python
import time
import schedule
import random
import uuid
from kafka import KafkaProducer
from json import dumps  
import os

def get_data():

    # Producer instance
    my_data = {"Hello World":"1"}
    #brokers = os.environ.get('KAFKA_BROKERS').split(',')
    prod = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer = lambda x:dumps(x).encode('utf-8'))
    print(my_data)
    prod.send(topic='input-topic', value=my_data)
    prod.flush()
    prod.close()

if __name__ == "__main__":

    get_data()
    schedule.every(1).seconds.do(get_data)

    while True:
        schedule.run_pending()
        time.sleep(1)