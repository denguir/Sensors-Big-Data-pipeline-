"""Example Kafka consumer."""

import os
import os.path as osp
from operator import add
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

KAFKA_BROKER_URL = 'localhost:9092'
ZOOKEEPER_URL = 'localhost:2181'
TOPIC = 'sensors'
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

def parse_data(line):
    '''parses a single line of data = 1 sensor measure'''
    s = line.strip().split()
    try:
        return [{'time': datetime.strptime(osp.join(s[0], s[1]), '%Y-%m-%d/%H:%M:%S'),
                'room_id': int(s[2].split('-')[0]),
                'data_id': int(s[2].split('-')[1]),
                'data': float(s[3]),
                'voltage': float(s[4])
               }]
    except Exception as err:
        print('Wrong line format (%s): %s' % (line, err))
        return []

    
if __name__ == '__main__':

    sc = SparkContext("local[2]", "SpeedLayer")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 6)
    inputStream = KafkaUtils.createStream(ssc, ZOOKEEPER_URL, 
                                    'streaming-consumer', {TOPIC: 1})
    data = inputStream.map(lambda x: x[1]).flatMap(parse_data)
    data.pprint()

    ssc.start()
    ssc.awaitTermination()



