#!/usr/bin/env python
import time
import schedule
import random
import uuid
from kafka import KafkaConsumer
from json import dumps, loads
import os

print("create consumer")
cons = KafkaConsumer('input-topic', bootstrap_servers=['localhost:9092'], value_deserializer = lambda x:loads(x).decode('utf-8'))
print(cons.subscription())
print("created consumer")
for m in cons:
    print(m.value)
    

print("end")