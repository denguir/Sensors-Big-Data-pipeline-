"""Batch Layer data processor:
Streams data from an HDFS folder and process many space aggregation and 
computes some basic statistical operations on these aggregated data.
The aggregated data and the RAW data are then transfered to OpenTSDB 
via an HTTP API"""

import os
import os.path as osp
import requests
import subprocess
import json
from operator import add
from datetime import datetime

import findspark
findspark.init()

#from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

HDFS_PATH = os.environ.get('HDFS_PATH')
OPENTSDB_URL = 'http://' + os.environ.get('OPENTSDB_URL')

def parse_data(line):
    '''Parses a single line of sensor data'''
    # mun_lut = json.loads('municipality.json')
    # print(mun_lut)
    s = line.strip().split()
    try:
        return [{'time': datetime.strptime(osp.join(s[0],s[1]), '%Y-%m-%d/%H:%M:%S'),
                 'mun': int(s[2].split('-')[0]),
                 'room_id': int(s[2].split('-')[1]),
                 'data_id': int(s[2].split('-')[2]),
                 'data': float(s[3]),
                 'voltage': float(s[4])
                 }]
    except Exception as err:
        print('Wrong line format (%s): %s' % (line, err))
        return []


def agg_to_json(data, measure, op, agg_type):
    '''Aggregate data by specifying the data type (measure),
    the operation done on these data (op) and the space level
    of aggregation (agg_type)'''
    mun = data[0]
    room_id = data[1]
    timestamp = data[2].timestamp()
    value = data[4] if op == 'COUNT' else data[3]
    out_data = {'tags':{}}
    out_data['metric'] = '%s' %measure
    out_data['timestamp'] = timestamp
    out_data['value'] = value
    if agg_type == 'city':
        out_data['tags']['city'] = 'Brussels'
    elif agg_type == 'municipality':
        out_data['tags']['city'] = 'Brussels'
        out_data['tags']['municipality'] = mun
    else:
        out_data['tags']['city'] = 'Brussels'
        out_data['tags']['municipality'] = mun
        out_data['tags']['space'] = room_id

    if op:
        out_data['groupByAggregator'] = op
    return out_data


def sendPartition(iter):
    iter = list(iter)
    if iter:
        r = requests.post(OPENTSDB_URL + '/api/put', data=json.dumps(iter))
        print('Raw data written to database with code', r.status_code)
        print(json.dumps(iter))
        return r.status_code
    else:
        r = 400
        #print(r)
        return r
    
def sendAggPartition(iter):
    iter = list(iter)
    if iter:
        r = requests.post(OPENTSDB_URL + '/api/rollup', data=json.dumps(iter))
        print('Agg data written to database with code', r.status_code)
        print(json.dumps(iter))
        return r.status_code
    else:
        r = 400
        #print(r)
        return r
    

    
if __name__ == '__main__':

    sc = SparkContext("local[*]", "BatchLayer")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 60)
    try:
       fileStream = ssc.textFileStream(HDFS_PATH)
    except:
       print('Last file not complete')
    
    sensorStream = fileStream.flatMap(parse_data)
    SENSOR_TYPES = ['temperature', 'humidity', 'light', 'motion']
    
    
    # Temperature Aggregated by city with MAX operation
    tempStream = sensorStream.filter(lambda d: d['data_id'] == 0)
    
    rawTempStream = tempStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data']))\
                                .map(lambda x: agg_to_json(x, 'temperature', None, None))\
                                .foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
    
    # rawTempStream.pprint()

    maxCityAggTempStream = tempStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: x if x[3] == max(x[3], y[3]) else y)\
                                     .map(lambda x: agg_to_json(x, 'temperature', 'MAX', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # Temperature Aggregated by municipality with MAX operation
    maxMunAggTempStream = tempStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: x if x[3] == max(x[3], y[3]) else y)\
                                    .mapValues(lambda x: agg_to_json(x, 'temperature', 'MAX', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # Temperature Aggregated by space with MIN operation
    maxSpaceAggTempStream = tempStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: x if x[3] == max(x[3], y[3]) else y)\
                                      .mapValues(lambda x: agg_to_json(x, 'temperature', 'MAX', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # maxMunAggTempStream.pprint()
    # maxSpaceAggTempStream.pprint()
    # maxCityAggTempStream.pprint()


    # Temperature Aggregated by city with MIN operation
    minCityAggTempStream = tempStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: x if x[3] == min(x[3], y[3]) else y)\
                                     .map(lambda x: agg_to_json(x, 'temperature', 'MIN', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # Temperature Aggregated by municipality with MIN operation
    minMunAggTempStream = tempStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: x if x[3] == min(x[3], y[3]) else y)\
                                    .mapValues(lambda x: agg_to_json(x, 'temperature', 'MIN', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # Temperature Aggregated by space with MIN operation
    minSpaceAggTempStream = tempStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: x if x[3] == min(x[3], y[3]) else y)\
                                      .mapValues(lambda x: agg_to_json(x, 'temperature', 'MIN', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # minMunAggTempStream.pprint()
    # minSpaceAggTempStream.pprint()
    # minCityAggTempStream.pprint()
    

    # Temperature Aggregated by city with SUM operation
    sumCityAggTempStream = tempStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: (x[0], x[1], x[2], x[3] + y[3]))\
                                     .map(lambda x: agg_to_json(x, 'temperature', 'SUM', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # Temperature Aggregated by municipality with SUM operation
    sumMunAggTempStream = tempStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3] + y[3]))\
                                    .mapValues(lambda x: agg_to_json(x, 'temperature', 'SUM', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # Temperature Aggregated by space with SUM operation
    sumSpaceAggTempStream = tempStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3] + y[3]))\
                                      .mapValues(lambda x: agg_to_json(x, 'temperature', 'SUM', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # sumMunAggTempStream.pprint()
    # sumSpaceAggTempStream.pprint()
    # sumCityAggTempStream.pprint()
    
   
    # Temperature Aggregated by city with COUNT operation
    countCityAggTempStream = tempStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: (x[0], x[1], x[2], x[3], x[4] + y[4]))\
                                     .map(lambda x: agg_to_json(x, 'temperature', 'COUNT', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # Temperature Aggregated by municipality with COUNT operation
    countMunAggTempStream = tempStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3], x[4] + y[4]))\
                                    .mapValues(lambda x: agg_to_json(x, 'temperature', 'COUNT', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # Temperature Aggregated by space with COUNT operation
    countSpaceAggTempStream = tempStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3], x[4] + y[4]))\
                                      .mapValues(lambda x: agg_to_json(x, 'temperature', 'COUNT', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # countMunAggTempStream.pprint()
    # countSpaceAggTempStream.pprint()
    # countCityAggTempStream.pprint()



    # humidity Aggregated by city with MAX operation
    HumStream = sensorStream.filter(lambda d: d['data_id'] == 1)
    
    rawHumStream = HumStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data']))\
                                .map(lambda x: agg_to_json(x, 'humidity', None, None))\
                                .foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
    
    # rawHumStream.pprint()

    maxCityAggHumStream = HumStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: x if x[3] == max(x[3], y[3]) else y)\
                                     .map(lambda x: agg_to_json(x, 'humidity', 'MAX', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # humidity Aggregated by municipality with MAX operation
    maxMunAggHumStream = HumStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: x if x[3] == max(x[3], y[3]) else y)\
                                    .mapValues(lambda x: agg_to_json(x, 'humidity', 'MAX', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # humidity Aggregated by space with MIN operation
    maxSpaceAggHumStream = HumStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: x if x[3] == max(x[3], y[3]) else y)\
                                      .mapValues(lambda x: agg_to_json(x, 'humidity', 'MAX', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # maxMunAggHumStream.pprint()
    # maxSpaceAggHumStream.pprint()
    # maxCityAggHumStream.pprint()


    # humidity Aggregated by city with MIN operation
    minCityAggHumStream = HumStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: x if x[3] == min(x[3], y[3]) else y)\
                                     .map(lambda x: agg_to_json(x, 'humidity', 'MIN', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # humidity Aggregated by municipality with MIN operation
    minMunAggHumStream = HumStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: x if x[3] == min(x[3], y[3]) else y)\
                                    .mapValues(lambda x: agg_to_json(x, 'humidity', 'MIN', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # humidity Aggregated by space with MIN operation
    minSpaceAggHumStream = HumStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: x if x[3] == min(x[3], y[3]) else y)\
                                      .mapValues(lambda x: agg_to_json(x, 'humidity', 'MIN', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # minMunAggHumStream.pprint()
    # minSpaceAggHumStream.pprint()
    # minCityAggHumStream.pprint()
    

    # humidity Aggregated by city with SUM operation
    sumCityAggHumStream = HumStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: (x[0], x[1], x[2], x[3] + y[3]))\
                                     .map(lambda x: agg_to_json(x, 'humidity', 'SUM', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # humidity Aggregated by municipality with SUM operation
    sumMunAggHumStream = HumStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3] + y[3]))\
                                    .mapValues(lambda x: agg_to_json(x, 'humidity', 'SUM', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # humidity Aggregated by space with SUM operation
    sumSpaceAggHumStream = HumStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3] + y[3]))\
                                      .mapValues(lambda x: agg_to_json(x, 'humidity', 'SUM', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # sumMunAggHumStream.pprint()
    # sumSpaceAggHumStream.pprint()
    # sumCityAggHumStream.pprint()
    
   
    # humidity Aggregated by city with COUNT operation
    countCityAggHumStream = HumStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: (x[0], x[1], x[2], x[3], x[4] + y[4]))\
                                     .map(lambda x: agg_to_json(x, 'humidity', 'COUNT', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # humidity Aggregated by municipality with COUNT operation
    countMunAggHumStream = HumStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3], x[4] + y[4]))\
                                    .mapValues(lambda x: agg_to_json(x, 'humidity', 'COUNT', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # humidity Aggregated by space with COUNT operation
    countSpaceAggHumStream = HumStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3], x[4] + y[4]))\
                                      .mapValues(lambda x: agg_to_json(x, 'humidity', 'COUNT', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # countMunAggHumStream.pprint()
    # countSpaceAggHumStream.pprint()
    # countCityAggHumStream.pprint()


    # light Aggregated by city with MAX operation
    LightStream = sensorStream.filter(lambda d: d['data_id'] == 2)
    
    rawLightStream = LightStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data']))\
                                .map(lambda x: agg_to_json(x, 'light', None, None))\
                                .foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
    
    # rawLightStream.pprint()

    maxCityAggLightStream = LightStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: x if x[3] == max(x[3], y[3]) else y)\
                                     .map(lambda x: agg_to_json(x, 'light', 'MAX', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # light Aggregated by municipality with MAX operation
    maxMunAggLightStream = LightStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: x if x[3] == max(x[3], y[3]) else y)\
                                    .mapValues(lambda x: agg_to_json(x, 'light', 'MAX', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # light Aggregated by space with MIN operation
    maxSpaceAggLightStream = LightStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: x if x[3] == max(x[3], y[3]) else y)\
                                      .mapValues(lambda x: agg_to_json(x, 'light', 'MAX', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # maxMunAggLightStream.pprint()
    # maxSpaceAggLightStream.pprint()
    # maxCityAggLightStream.pprint()


    # light Aggregated by city with MIN operation
    minCityAggLightStream = LightStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: x if x[3] == min(x[3], y[3]) else y)\
                                     .map(lambda x: agg_to_json(x, 'light', 'MIN', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # light Aggregated by municipality with MIN operation
    minMunAggLightStream = LightStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: x if x[3] == min(x[3], y[3]) else y)\
                                    .mapValues(lambda x: agg_to_json(x, 'light', 'MIN', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # light Aggregated by space with MIN operation
    minSpaceAggLightStream = LightStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: x if x[3] == min(x[3], y[3]) else y)\
                                      .mapValues(lambda x: agg_to_json(x, 'light', 'MIN', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # minMunAggLightStream.pprint()
    # minSpaceAggLightStream.pprint()
    # minCityAggLightStream.pprint()
    

    # light Aggregated by city with SUM operation
    sumCityAggLightStream = LightStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: (x[0], x[1], x[2], x[3] + y[3]))\
                                     .map(lambda x: agg_to_json(x, 'light', 'SUM', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # light Aggregated by municipality with SUM operation
    sumMunAggLightStream = LightStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3] + y[3]))\
                                    .mapValues(lambda x: agg_to_json(x, 'light', 'SUM', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # light Aggregated by space with SUM operation
    sumSpaceAggLightStream = LightStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3] + y[3]))\
                                      .mapValues(lambda x: agg_to_json(x, 'light', 'SUM', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # sumMunAggLightStream.pprint()
    # sumSpaceAggLightStream.pprint()
    # sumCityAggLightStream.pprint()
    
   
    # light Aggregated by city with COUNT operation
    countCityAggLightStream = LightStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: (x[0], x[1], x[2], x[3], x[4] + y[4]))\
                                     .map(lambda x: agg_to_json(x, 'light', 'COUNT', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # light Aggregated by municipality with COUNT operation
    countMunAggLightStream = LightStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3], x[4] + y[4]))\
                                    .mapValues(lambda x: agg_to_json(x, 'light', 'COUNT', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # light Aggregated by space with COUNT operation
    countSpaceAggLightStream = LightStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3], x[4] + y[4]))\
                                      .mapValues(lambda x: agg_to_json(x, 'light', 'COUNT', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # countMunAggLightStream.pprint()
    # countSpaceAggLightStream.pprint()
    # countCityAggLightStream.pprint()


    # motion Aggregated by city with MAX operation
    motionStream = sensorStream.filter(lambda d: d['data_id'] == 3)
    
    rawmotionStream = motionStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data']))\
                                .map(lambda x: agg_to_json(x, 'motion', None, None))\
                                .foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
    
    # rawmotionStream.pprint()

    maxCityAggmotionStream = motionStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: x if x[3] == max(x[3], y[3]) else y)\
                                     .map(lambda x: agg_to_json(x, 'motion', 'MAX', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # motion Aggregated by municipality with MAX operation
    maxMunAggmotionStream = motionStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: x if x[3] == max(x[3], y[3]) else y)\
                                    .mapValues(lambda x: agg_to_json(x, 'motion', 'MAX', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # motion Aggregated by space with MIN operation
    maxSpaceAggmotionStream = motionStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: x if x[3] == max(x[3], y[3]) else y)\
                                      .mapValues(lambda x: agg_to_json(x, 'motion', 'MAX', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # maxMunAggmotionStream.pprint()
    # maxSpaceAggmotionStream.pprint()
    # maxCityAggmotionStream.pprint()


    # motion Aggregated by city with MIN operation
    minCityAggmotionStream = motionStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: x if x[3] == min(x[3], y[3]) else y)\
                                     .map(lambda x: agg_to_json(x, 'motion', 'MIN', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # motion Aggregated by municipality with MIN operation
    minMunAggmotionStream = motionStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: x if x[3] == min(x[3], y[3]) else y)\
                                    .mapValues(lambda x: agg_to_json(x, 'motion', 'MIN', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # motion Aggregated by space with MIN operation
    minSpaceAggmotionStream = motionStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: x if x[3] == min(x[3], y[3]) else y)\
                                      .mapValues(lambda x: agg_to_json(x, 'motion', 'MIN', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # minMunAggmotionStream.pprint()
    # minSpaceAggmotionStream.pprint()
    # minCityAggmotionStream.pprint()
    

    # motion Aggregated by city with SUM operation
    sumCityAggmotionStream = motionStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: (x[0], x[1], x[2], x[3] + y[3]))\
                                     .map(lambda x: agg_to_json(x, 'motion', 'SUM', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # motion Aggregated by municipality with SUM operation
    sumMunAggmotionStream = motionStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3] + y[3]))\
                                    .mapValues(lambda x: agg_to_json(x, 'motion', 'SUM', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # motion Aggregated by space with SUM operation
    sumSpaceAggmotionStream = motionStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3] + y[3]))\
                                      .mapValues(lambda x: agg_to_json(x, 'motion', 'SUM', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # sumMunAggmotionStream.pprint()
    # sumSpaceAggmotionStream.pprint()
    # sumCityAggmotionStream.pprint()
    
   
    # motion Aggregated by city with COUNT operation
    countCityAggmotionStream = motionStream.map(lambda d: (d['mun'], d['room_id'], d['time'], d['data'], 1))\
                                     .reduce(lambda x,y: (x[0], x[1], x[2], x[3], x[4] + y[4]))\
                                     .map(lambda x: agg_to_json(x, 'motion', 'COUNT', 'city'))\
                                     .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # motion Aggregated by municipality with COUNT operation
    countMunAggmotionStream = motionStream.map(lambda d: (d['mun'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                    .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3], x[4] + y[4]))\
                                    .mapValues(lambda x: agg_to_json(x, 'motion', 'COUNT', 'municipality'))\
                                    .map(lambda x: x[1])\
                                    .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # motion Aggregated by space with COUNT operation
    countSpaceAggmotionStream = motionStream.map(lambda d: (d['room_id'], (d['mun'], d['room_id'], d['time'], d['data'], 1)))\
                                      .reduceByKey(lambda x,y: (x[0], x[1], x[2], x[3], x[4] + y[4]))\
                                      .mapValues(lambda x: agg_to_json(x, 'motion', 'COUNT', 'space'))\
                                      .map(lambda x: x[1])\
                                      .foreachRDD(lambda rdd: rdd.foreachPartition(sendAggPartition))

    # countMunAggmotionStream.pprint()
    # countSpaceAggmotionStream.pprint()
    # countCityAggmotionStream.pprint()

    
    ssc.start()
    ssc.awaitTermination()
