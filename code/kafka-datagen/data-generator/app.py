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

def generate_data(data_id):
    """Generate a random distribution depending on data_id:
    data_id = 0 --> temperature
    data_id = 1 --> humidity
    data_id = 2 --> light
    """
    assert data_id in range(4)
    if data_id == 0:
        # temperature
        data = np.random.normal(20., 5.)
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
    data_format = "{:{dfmt} {tfmt}} {municipality};{room_id};{data_id} {data} {volt}"
    municipalities = get_municipalities('municipalities.txt')
    while True:
        now = datetime.now()
        room_id = np.random.randint(1, 54)
        voltage = np.around(np.random.uniform(0., 5.), decimals=4)
        for i in range(4):
            sensor_data = generate_data(i)
            data = data_format.format(now, dfmt='%Y-%m-%d', tfmt='%H:%M:%S', municipality=municipalities[room_id], 
                                room_id=room_id, data_id=i, data=sensor_data, volt=voltage)
            print(data)
            producer.send(TOPIC, key=bytes([room_id]), value=bytes(data, encoding='utf-8'))
        producer.flush()
        time.sleep(SLEEP_TIME)

    