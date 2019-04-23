"""Example HDFS consumer."""

import os
import os.path as osp
import pandas as pd
from operator import add
from datetime import datetime

import findspark
findspark.init()

from pyspark.sql import SparkSession
HDFS_PATH = os.environ.get('HDFS_PATH')

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

def day_key(d):
    return 10000*d['time'].year + 100*d['time'].month + d['time'].day

def slot_key(d, size):
    return (60*d['time'].hour + d['time'].minute)%size

if __name__ == '__main__':
    spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("BatchLayer") \
            .getOrCreate()
    sc = spark.sparkContext
    # open the whole directoty
    print(HDFS_PATH)
    sensorsRDD = sc.textFile(HDFS_PATH)
    # temperature data
    tempRDD = sensorsRDD.flatMap(parse_data)\
                        .filter(lambda d: d['data_id'] == 0)

    # group by (day, slot) making integer encoding of day as a key
    slotTempRDD = tempRDD.map(lambda d: (day_key(d), slot_key(d, 15), d))\
                        .groupBy(lambda d: (d[0], d[1]))\
                        .mapValues(lambda x: [y for y in x])

    print(slotTempRDD.collect())

