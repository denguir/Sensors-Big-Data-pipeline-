"""Example Kafka consumer."""

import os
import os.path as osp
import pandas as pd
from operator import add
from datetime import datetime
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
ZOOKEEPER_URL = os.environ.get('ZOOKEEPER_URL')
TOPIC = os.environ.get('TOPIC')


def parse_data(line):
    '''Parses a single line of sensor data'''
    s = line.strip().split()
    try:
        return [{'time': datetime.strptime(osp.join(s[0],s[1]), '%Y-%m-%d/%H:%M:%S'),
                 'room_id': int(s[2].split('-')[0]),
                 'data_id': int(s[2].split('-')[1]),
                 'data': float(s[3]),
                 'voltage': float(s[4])
                 }]
    except Exception as err:
        print('Wrong line format (%s): %s' % (line, err))
        return []


if __name__ == '__main__':

    sc = SparkContext("local[*]", "SpeedLayer")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5)
    inputStream = KafkaUtils.createStream(ssc, ZOOKEEPER_URL, 
                                        'streaming-consumer', {TOPIC: 1})
    
    # data comes from Kafka as a key, value pairs
    data = inputStream.map(lambda x: x[1]).flatMap(parse_data).collect()
    data.pprint()
'''
    temp_freq = data.filter(lambda d: d['data_id'] == 0)\
                    .groupBy(lambda d: round(d['temp'], 1))\
                    .map(lambda v: 1).window(3600, 60)\
                    .reduce(add)
    temp_freq.pprint()
'''


