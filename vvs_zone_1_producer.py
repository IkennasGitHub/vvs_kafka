import pandas as pd
from vvspy import get_departures
import json, time
from confluent_kafka import Producer
from vvspy.enums import Station
from datetime import datetime, timedelta
import vvspy.enums
from multiprocessing import Process

# configure the Kafka producer
producer = Producer({'bootstrap.servers': 'localhost:9092'})

# Load the CSV file into a pandas DataFrame
df = pd.read_csv('vvs_steige.csv', delimiter=';', encoding='latin1')

# Extract 'Globale ID' where 'Tarifzonen' is 1
filtered_ids = df[df['Tarifzonen'] == '1']['Globale ID'].tolist()

# Print the list of filtered IDs
#print(filtered_ids)

data_file = "rt_data_old.json"

zone_list = []
sttn_list = list(Station.__members__.values())
for index in range(len(sttn_list)):
    if sttn_list[index].value in filtered_ids:
        zone_list.append(sttn_list[index])

current_time = datetime.now()

def get_transit_info(start_index, end_index, requested_time, stop_time):
    for index in range(start_index, end_index):
        if index >= len(zone_list):
            break
        
        deps = get_departures(zone_list[index], limit=30, check_time=requested_time)
        print(f"Current iteration: {zone_list[index].name}, index: {index}")
            
        for dep in deps:
            if dep.real_datetime < stop_time:
                dep_info = {
                "station": zone_list[index].name,
                "serving_line": dep.serving_line.symbol,
                "year": dep.real_datetime.year,
                "month": dep.real_datetime.month,
                "day": dep.real_datetime.day,
                "time": dep.real_datetime.strftime('%H:%M'),
                "delay": dep.delay,
                "cancelled": dep.cancelled,
                "status": ""
                }
                if dep.delay > 0:
                    dep_info["status"] = "Delayed"
                    producer.produce("vvs_rt_data", key=zone_list[index].name, value=json.dumps(dep_info))
                    print(f"Alarm! Delay detected at {zone_list[index].name}")
                    print(dep)
            
                    #f.write(f'{dep.serving_line}; {dep.delay}\n')
                    print(f'There will be a {dep.delay} minutes delay at {zone_list[index].name}')
                    with open(data_file, 'a', encoding='utf-8') as json_file:
                        json.dump(dep_info, json_file,ensure_ascii=False, indent=4)
                elif dep.cancelled:
                    dep_info["status"] = "Cancelled"
                    producer.produce("vvs_rt_data", key=zone_list[index].name, value=json.dumps(dep_info))
                    print(f"Alarm! The train at {dep.real_datetime} has been cancelled!")
                    with open(data_file, 'a', encoding='utf-8') as json_file:
                        json.dump(dep_info, json_file, ensure_ascii=False, indent=4)
                else:
                    dep_info["status"] = "On time"
                    producer.produce("vvs_rt_data", key=zone_list[index].name, value=json.dumps(dep_info))
                    print(f"Train on time at {zone_list[index].name}")
                    print(dep.real_datetime.strftime('%H:%M'))
                    continue
            else:
                continue

if __name__ == '__main__':
    while True:
        with open('current_time.json', 'r') as file:
            data = json.load(file)
            stored_time_str = data["current_time"]
            requested_time = datetime.strptime(stored_time_str, '%Y-%m-%d %H:%M:%S.%f')
            stop_time = requested_time + timedelta(minutes=5)
        
        start_time = time.time()
        process_1 = Process(target=get_transit_info, args=(0, 310, requested_time, stop_time))
        process_2 = Process(target=get_transit_info, args=(310, 620, requested_time, stop_time))
        process_3 = Process(target=get_transit_info, args=(620, 930, requested_time, stop_time))
        process_4 = Process(target=get_transit_info, args=(930, 1240, requested_time, stop_time))

        process_1.start()
        process_2.start()
        process_3.start()
        process_4.start()

        process_1.join()
        process_2.join()
        process_3.join()
        process_4.join()

        with open('current_time.json', 'w') as file:
            json.dump({"current_time": stop_time.strftime('%Y-%m-%d %H:%M:%S.%f')}, file)
        
        print(f"Processing complete in {time.time() - start_time} seconds")

        while True:
            if stop_time >= (datetime.now() + timedelta(minutes=5)):
               print('Process waiting...')
               time.sleep(3 * 60)
               
            else:
                break 
