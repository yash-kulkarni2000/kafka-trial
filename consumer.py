from kafka import KafkaConsumer
import sys

bootstrap_servers = ['localhost:9092']
topicName = 'input-topic'

# Initialize consumer variable
consumer = KafkaConsumer (topicName, group_id ='consumer-group-01',bootstrap_servers=bootstrap_servers)

# Read and print message from consumer
for msg in consumer:
   print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, msg.value, msg.timestamp)

# Terminate the script
sys.exit()
