import os
import pwd
import time
import numpy as np
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
DATA_PER_SECOND = os.environ.get('DATA_PER_SECOND')
TOPIC = os.environ.get('TOPIC')
SLEEP_TIME = 1. / float(DATA_PER_SECOND)
NUM_SENSORS = 54
NUM_TYPE_SENSORS = 4

def generate_data(data_id, max_id):
    """Generate a random distribution depending on data_id:
    data_id = 0 --> temperature
    data_id = 1 --> humidity
    data_id = 2 --> light
    """
    assert data_id in range(max_id)
    if data_id == 0:
        # temperature
        data = np.random.normal(20., 2.)
    elif data_id == 1:
        # humidity
        data = np.random.uniform(0., 100.)
    elif data_id == 2:
        # light
        data = np.random.normal(40., 5.)
    else:
        # motion
        data = np.random.randint(0, 2)
    data = np.around(data, decimals=4)
    return data

def get_locs(file):
    '''Extracts coordinates of sensors from a txt file'''
    lut = dict()
    with open(file, 'r') as f:
        locs = f.readlines()
    locs = list(map(lambda line: line.strip().split(), locs))
    for data in locs:
    	lut[int(data[0])] = (float(data[1]), float(data[2]))
    return lut

def get_municipalities(file):
    '''Extracts municipality of each sensor from a txt file'''
    lut = dict()
    with open(file, 'r') as f:
        m = f.readlines()
    m = list(map(lambda line: line.strip().split(','), m))
    for data in m:
        lut[int(data[0])] = data[1]
    return lut

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER_URL], api_version=(0, 10, 1))
    data_format = "{:{dfmt} {tfmt}} {mun_id}-{room_id}-{data_id} {data} {volt}"
    while True:
        now = datetime.now()
        # room_id = np.random.randint(1, NUM_SENSORS)
        # mun_id = room_id % 19 # there is 19 municipalities in Brussels
        for room_id in range(NUM_SENSORS):
            mun_id = room_id % 19 # there is 19 municipalities in Brussels
            for i in range(NUM_TYPE_SENSORS):
                if np.random.randint(0, 100) > 30:
                    voltage = np.around(np.random.uniform(0., 5.), decimals=4)
                    sensor_data = generate_data(i, NUM_TYPE_SENSORS)
                    data = data_format.format(now, dfmt='%Y-%m-%d', tfmt='%H:%M:%S', mun_id=mun_id, 
                                        room_id=room_id, data_id=i, data=sensor_data, volt=voltage)
                    print(data)
            producer.send(TOPIC, key=bytes([room_id]), value=bytes(data, encoding='utf-8'))
        producer.flush()
        time.sleep(SLEEP_TIME)
        

    