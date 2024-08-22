from vvspy import get_departures
from confluent_kafka import Producer
import time
import json
from vvspy.enums import Station
from datetime import datetime, timedelta
import vvspy.enums

# configure the Kafka producer
producer = Producer({'bootstrap.servers': 'localhost:9092'})

#f = open("realtime_data.csv", "a")
station_list = list(Station.__members__.values())

current_time = datetime.now()
requested_time = current_time - timedelta(hours=0)
stop_time = requested_time + timedelta(hours=1)

for index in range(1000):
        
    deps = get_departures(station_list[index], limit=40, check_time=requested_time)
    #print(f"Current iteration: {station_list[index].name}")
        
    for dep in deps:
        if dep.real_datetime < stop_time:
            if dep.serving_line.symbol in ("S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10", "U1", "U2", "U3", "U4", "U5", "U6", "U7", "U8", "U9", "U10", "U11", "U12", "U13", "U14"):
                dep_info = {
                    "station": station_list[index].name,
                    "serving_line": dep.serving_line.symbol,
                    #"time": dep.real_datetime.isoformat() if dep.real_datetime else None,
                    "year": dep.real_datetime.year,
                    "month": dep.real_datetime.month,
                    "day": dep.real_datetime.day,
                    "time": dep.real_datetime.strftime('%H:%M'),
                    "delay": dep.delay,
                    "cancelled": dep.cancelled,
                    "status": ""
                }
                if dep.delay > 0:
                    '''print(f"Alarm! Delay detected at {station_list[index].name}")
                    print(dep)
            
                    #f.write(f'{dep.serving_line}; {dep.delay}\n')
                    print(f'There will be a {dep.delay} minutes delay at {station_list[index].name}')'''
                    
                    dep_info["status"] = "Delayed"
                    producer.produce("vvs_rt_data", key=station_list[index].name, value=json.dumps(dep_info))
                elif dep.cancelled:
                    #print(f"Alarm! The train at {dep.real_datetime} has been cancelled!")
                    dep_info["status"] = "Cancelled"
                    producer.produce("vvs_rt_data", key=station_list[index].name, value=json.dumps(dep_info))
                else:
                    #print(f"Train on time at {station_list[index].name}")
                    #print(dep)
                    continue
                    
            else:
                continue
        else:
            continue
