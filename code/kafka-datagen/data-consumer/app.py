"""Example Kafka consumer."""

import os
from kafka import KafkaConsumer

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TOPIC = os.environ.get('TOPIC')


if __name__ == '__main__':
    data_format = "{:{dfmt} {tfmt}} {room_id}-{data_id} {data} {volt}"
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        api_version=(0, 10, 1)
    )

    for message in consumer:
        print ("%s:%d:%d: key=%d value=%s" % (message.topic, message.partition, message.offset, int.from_bytes(message.key, 'big'), str(message.value))) 
