"""Example Kafka consumer."""

import os
import pandas as pd
from hdfs import InsecureClient
from kafka import KafkaConsumer

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TOPIC = os.environ.get('TOPIC')
HDFS_URL = os.environ.get('HDFS_URL')
BATCH_SIZE = os.environ.get('BATCH_SIZE')

if __name__ == '__main__':
    hdfs_path = '/hadoop-data'
    client_hdfs = InsecureClient(HDFS_URL)
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        api_version=(0, 10, 1)
    )

    for message in consumer:
        key = int.from_bytes(message.key, 'big')
        value = message.value.decode('utf-8')
        # typically, must serialize data before feeding it to HDFS (using dataframe pandas maybe)
        print("%s:%d:%d: key=%d value=%s" % (message.topic, message.partition, message.offset, key, value))
        ext = '.txt'
        filename = str(message.topic) + '_' + str(message.partition) + '_' + str(key) + ext
        path = os.path.join(hdfs_path, filename)
        client_hdfs.write(path, data=value, append=True, encoding='utf-8')

