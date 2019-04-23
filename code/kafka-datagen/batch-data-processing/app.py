"""Example HDFS consumer."""

import os
import os.path as osp
import pandas as pd
import subprocess
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
    return (60*d['time'].hour + d['time'].minute)//size


def run_cmd(args_list):
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    (output, errors) = proc.communicate()
    if proc.returncode:
        raise RuntimeError(
            'Error running command: %s. Return code: %d, Error: %s' % (
                ' '.join(args_list), proc.returncode, errors))
    return (output, errors)

def ls(hdfs_url):
    """
    List the hdfs URL.  If the URL is a directory, the contents are returned.
    full=True ensures hdfs:// is prepended
    """
    out, err = run_cmd(['hadoop', 'fs', '-ls', hdfs_url])
    flist = []
    lines = out.decode('utf-8').split('\n')
    for line in lines:
        ls = line.split()
        if len(ls) == 8:
            # this is a file description line
            fname=ls[-1]
            fname = fname.split('/')[-1]
            flist.append(fname)

    return flist

def select_files(path, thrd):
    '''Select the most recent files from the HDFS path
    with respect to a given time threshold
    expected input example: FlumeData.1556035720679'''
    files = ls(path)
    selected_files = list(map(lambda f: (int(f.strip().split('.')[1]), osp.join(path, f)), files))
    selected_files = list(filter(lambda pair: pair[0] > thrd, selected_files))
    selected_files = list(map(lambda pair: pair[1], selected_files))
    f = ','.join(selected_files)
    return f



if __name__ == '__main__':
    spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("BatchLayer") \
            .getOrCreate()
    sc = spark.sparkContext

    # open the whole directoty and select the most recent files (30 min old) with 2h correction
    time_thrd = int((datetime.now().timestamp() - 30 * 60 - 2 * 3600) * 1000) # *1000 because 3rd decimal precision
    try:
        hdfs_files = select_files(HDFS_PATH, time_thrd)
    except:
        print('No data within the past 30 mins')
        
    sensorsRDD = sc.textFile(hdfs_files)
    # temperature data
    tempRDD = sensorsRDD.flatMap(parse_data)\
                        .filter(lambda d: d['data_id'] == 0)

    # group by (day, slot) making integer encoding of day as a key
    slotTempRDD = tempRDD.map(lambda d: (day_key(d), slot_key(d, 15), d))\
                        .groupBy(lambda d: (d[0], d[1]))\
                        .mapValues(lambda x: [y for y in x])

    print(slotTempRDD.collect())

