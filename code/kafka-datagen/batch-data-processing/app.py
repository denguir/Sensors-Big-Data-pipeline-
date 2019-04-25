"""Example HDFS consumer."""

import os
import os.path as osp
import requests
import subprocess
from operator import add
from datetime import datetime

import findspark
findspark.init()

#from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
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
    return (60*d['time'].hour + d['time'].minute)//size


# def run_cmd(args_list):
#     proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
#             stderr=subprocess.PIPE)
#     (output, errors) = proc.communicate()
#     if proc.returncode:
#         raise RuntimeError(
#             'Error running command: %s. Return code: %d, Error: %s' % (
#                 ' '.join(args_list), proc.returncode, errors))
#     return (output, errors)

# def ls(hdfs_url):
#     """
#     List the hdfs URL.  If the URL is a directory, the contents are returned.
#     full=True ensures hdfs:// is prepended
#     """
#     out, err = run_cmd(['hadoop', 'fs', '-ls', hdfs_url])
#     flist = []
#     lines = out.decode('utf-8').split('\n')
#     for line in lines:
#         ls = line.split()
#         if len(ls) == 8:
#             # this is a file description line
#             fname=ls[-1]
#             fname = fname.split('/')[-1]
#             flist.append(fname)
#     return flist

# def select_files(path, thrd):
#     '''Select the most recent files from the HDFS path
#     with respect to a given time threshold
#     expected input example: FlumeData.1556035720679'''
#     files = ls(path)
#     selected_files = list(map(lambda f: (int(f.strip().split('.')[1]), osp.join(path, f)), files))
#     selected_files = list(filter(lambda pair: pair[0] > thrd, selected_files))
#     selected_files = list(map(lambda pair: pair[1], selected_files))
#     f = ','.join(selected_files)
#     return f

def post_data(db, data):
    r = requests.post(db, data=data)
    return r.status_code

if __name__ == '__main__':
    sc = SparkContext("local[*]", "BatchLayer")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 10)

    sensorStream = ssc.textFileStream(HDFS_PATH)

    print(datetime.now())

    dataStream = sensorStream.flatMap(parse_data)
    # temperature data
    tempStream = dataStream.filter(lambda d: d['data_id'] == 0)
    # humidity data
    humidityStream = dataStream.filter(lambda d: d['data_id'] == 1)
    # light data
    lightStream = dataStream.filter(lambda d: d['data_id'] == 2)
    # motion data
    motionStream = dataStream.filter(lambda d: d['data_id'] == 3)

    # group by (day, slot) making integer encoding of day as a key
    slotTempStream = tempStream.map(lambda d: (day_key(d), slot_key(d, 15), d))\
                        .transform(lambda rdd: rdd.groupBy(lambda d: (d[0], d[1]))\
                            .mapValues(lambda x: (sum([y['data'] for y in x]), len([y['data'] for y in x])))\
                            .mapValues(lambda x: 0 if x[0]/x[1] < 19.5 else 1)
                        )
    slotTempStream.pprint()

    aggDataStream = dataStream.window(3600, 10)
    # temperature data
    aggTempStream = aggDataStream.filter(lambda d: d['data_id'] == 0)
    minAggTempStream = aggTempStream.map(lambda temp: min(temp))
    maxAggTempStream = aggTempStream.map(lambda temp: max(temp))
    meanAggTempStream = aggTempStream.map(lambda temp: sum(temp)/len(temp))
    # humidity data
    aggHumidityStream = aggDataStream.filter(lambda d: d['data_id'] == 1)
    minAggHumidityStream = aggHumidityStream.map(lambda temp: min(temp))
    maxAggHumidityStream = aggHumidityStream.map(lambda temp: max(temp))
    meanAggHumidityStream = aggHumidityStream.map(lambda temp: sum(temp)/len(temp))
    # light data
    aggLightStream = aggDataStream.filter(lambda d: d['data_id'] == 2)
    minAggLightStream = aggLightStream.map(lambda temp: min(temp))
    maxAggLightStream = aggLightStream.map(lambda temp: max(temp))
    meanAggLightStream = aggLightStream.map(lambda temp: sum(temp)/len(temp))
    # motion data
    aggMotionStream = aggDataStream.filter(lambda d: d['data_id'] == 3)
    minAggMotionStream = aggMotionStream.map(lambda temp: min(temp))
    maxAggMotionStream = aggMotionStream.map(lambda temp: max(temp))
    meanAggMotionStream = aggMotionStream.map(lambda temp: sum(temp)/len(temp))

    ssc.start()
    ssc.awaitTermination()

    # THE ERROR IS PROBABLY DUE TO THE LAST FLUME DATA (WHICH IS AN EMPTY ONE)
