"""Example HDFS consumer."""

import os
import os.path as osp
import pandas as pd
from operator import add
from datetime import datetime

import findspark
findspark.init()

from pyspark.sql import SparkSession
HDFS_URL = os.environ('HDFS_URL')

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
    spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("BatchLayer") \
            .getOrCreate()
    sc = spark.sparkContext
    # open the whole directoty
    sensorsRDD = sc.textFile(osp.join(HDFS_URL, 'user', 'data', 'sensors'))
    # temperature data
    tempRDD = sensorsRDD.flatMap(parse_data)\
                        .filter(lambda d: d['data_id'] == 0)

    # group by day, making integer encoding of day as a key
    dailyTempRDD = tempRDD.map(lambda d: (10000*d['time'].year + 100*d['time'].month + d['time'], d))\
                            .groupByKey()
    # group by 15 min time slot 
    #slotTempRDD = dailyTempRDD.mapValues(lambda value: ())
    


