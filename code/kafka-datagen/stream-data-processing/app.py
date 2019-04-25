"""Example Kafka consumer."""

import os
import os.path as osp
import requests
from operator import add
from datetime import datetime

#os.environ['PYSPARK_SUBMIT_ARGS'] = "--jars " + osp.join(os.environ['SPARK_HOME'], "jars", "spark-streaming-kafka-0-8-assembly_2.11-2.4.1.jar") + " pyspark-shell"
import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
ZOOKEEPER_URL = os.environ.get('ZOOKEEPER_URL')
TOPIC = os.environ.get('TOPIC')
OPENTSDB_URL = 'http://' + os.environ.get('OPENTSDB_URL')


def parse_data(line):
    '''Parses a single line of sensor data'''
    s = line.strip().split()
    try:
        return [{'time': datetime.strptime(osp.join(s[0],s[1]), '%Y-%m-%d/%H:%M:%S'),
                 'mun': str(s[2].split(';')[0]),
                 'room_id': int(s[2].split(';')[1]),
                 'data_id': int(s[2].split(';')[2]),
                 'data': float(s[3]),
                 'voltage': float(s[4])
                 }]
    except Exception as err:
        print('Wrong line format (%s): %s' % (line, err))
        return []

def to_json(data_and_occ):
    data = list(data_and_occ[0])
    occ = data_and_occ[1]
    out_data = [0] * len(data)
    out_occ = [0] * len(data)
    
    for i in range(len(data)):
        out_data[i] = {'tags': {}}
        out_data[i]['metric'] = 'temperature.occurence'
        out_data[i]['timestamp'] = data[i]['time'].timestamp()
        out_data[i]['value'] = data[i]['data']
        out_data[i]['tags']['space'] = data[i]['room_id']
        out_data[i]['tags']['municipality'] = data[i]['mun']

        out_occ[i] = {'tags': {}}
        out_occ[i]['metric'] = 'occurence.temperature'
        out_occ[i]['timestamp'] = data[i]['time'].timestamp()
        out_occ[i]['value'] = occ
        out_occ[i]['tags']['space'] = data[i]['room_id']
        out_occ[i]['tags']['municipality'] = data[i]['mun']
    
    return out_data + out_occ

def post_data(db, data):
    r = requests.post(db, data=data)
    print(r.status_code)

if __name__ == '__main__':

    sc = SparkContext("local[*]", "SpeedLayer")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5)
    inputStream = KafkaUtils.createStream(ssc, ZOOKEEPER_URL, 
                                        'streaming-consumer', {TOPIC: 1})
    
    # data comes from Kafka as a key, value pairs
    data = inputStream.map(lambda x: x[1]).flatMap(parse_data)

    # select temperature data and calculate frequency of each temperature
    # in a window of 1 hour (1 min update) and return top 10 best temperature
    top_10_temp = data.filter(lambda d: d['data_id'] == 0)\
                        .transform(lambda rdd: rdd.groupBy(lambda d: round(d['data'], 1)))\
                        .mapValues(lambda d: (d, len(d)))\
                        .window(60, 5)\
                        .transform(lambda rdd: rdd.sortBy(lambda v: v[1][1], False)\
                            .zipWithIndex()\
                            .filter(lambda idx: idx[1] < 10))\
                        .map(lambda x: to_json(x[0][1]))\
                        .transform(lambda rdd: post_data(OPENTSDB_URL, rdd))

    top_10_temp.pprint()

    ssc.start()
    ssc.awaitTermination()


