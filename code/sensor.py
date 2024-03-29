import os
import pwd
import time
import numpy as np
from kafka import KafkaProducer
from datetime import datetime
from json import dumps


def generate_data(data_id):
    """Generate a random distribution depending on data_id:
    data_id = 0 --> temperature
    data_id = 1 --> humidity
    data_id = 2 --> light
    """
    assert data_id in range(3)
    if data_id == 0:
        # temperature
        data = np.random.normal(20., 5.)
    elif data_id == 1:
        # humidity
        data = np.random.uniform(0., 100.)
    else:
        # light
        data = np.random.normal(40., 5.)
    data = np.around(data, decimals=4)
    return data

if __name__ == '__main__':
    topic = pwd.getpwuid(os.getuid())[0] # topic id
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10, 2))
    data_format = "{:{dfmt} {tfmt}} {room_id}-{data_id} {data} {volt}"
    while True:
        now = datetime.now()
        room_id = np.random.randint(1, 54)
        voltage = np.around(np.random.uniform(0., 5.), decimals=4)
        for i in range(3):
            sensor_data = generate_data(i)
            data = data_format.format(now, dfmt='%Y-%m-%d', tfmt='%H:%M:%S', room_id=room_id, 
                                    data_id=i, data=sensor_data, volt=voltage)
            print(data)
            producer.send(topic, key=bytes(room_id), value=bytes(data))
        producer.flush()
        time.sleep(3)

    