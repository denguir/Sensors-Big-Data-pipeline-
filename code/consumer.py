import os
import pwd
from kafka import KafkaConsumer

topic = pwd.getpwuid( os.getuid() )[ 0 ]
consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'], api_version=(0, 10, 1))

for message in consumer:
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value)) 
