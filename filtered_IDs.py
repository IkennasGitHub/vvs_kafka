import pandas as pd
from vvspy import get_departures
import json, time
from vvspy.enums import Station
from datetime import datetime, timedelta
import vvspy.enums
from multiprocessing import Process

# Load the CSV file into a pandas DataFrame
# Replace 'your_file.csv' with the actual path to your CSV file
df = pd.read_csv('vvs_steige.csv', delimiter=';', encoding='latin1')

# Extract 'Globale ID' where 'Tarifzonen' is 1
filtered_ids = df[df['Tarifzonen'] == '1']['Globale ID'].tolist()

# Print the list of filtered IDs
#print(filtered_ids)

zone_list = []
sttn_list = list(Station.__members__.values())
for index in range(len(sttn_list)):
    if sttn_list[index].value in filtered_ids:
        zone_list.append(sttn_list[index])

current_time = datetime.now()
requested_time = current_time - timedelta(minutes = 5)
stop_time = requested_time + timedelta(minutes = 5)

def get_transit_info(start_index, end_index):
    for index in range(start_index, end_index):
        if index >= len(zone_list):
            break

    #for index in range(200):
        
        deps = get_departures(zone_list[index], limit=5, check_time=requested_time)
        print(f"Current iteration: {zone_list[index].name}, index: {index}")
            
        for dep in deps:
            if dep.real_datetime < stop_time:
                if dep.serving_line.symbol in ("S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10", "U1", "U2", "U3", "U4", "U5", "U6", "U7", "U8", "U9", "U10", "U11", "U12", "U13", "U14"):
                    if dep.delay > 0:
                        print(f"Alarm! Delay detected at {zone_list[index].name}")
                        print(dep)
                
                        #f.write(f'{dep.serving_line}; {dep.delay}\n')
                        print(f'There will be a {dep.delay} minutes delay at {zone_list[index].name}')
                    elif dep.cancelled:
                        print(f"Alarm! The train at {dep.real_datetime} has been cancelled!")
                    else:
                        #print(f"Train on time at {zone_list[index].name}")
                        #print(dep.real_datetime.strftime('%H:%M'))
                        continue
                else:
                    continue
            else:
                continue

if __name__ == '__main__':
    # Define the ranges for different processes
    ranges = [(0, 400), (400, 900)]

    # Create a list of processes
    '''processes = []
    for start, end in ranges:
        p = multiprocessing.Process(target=get_transit_info, args=(start, end))
        processes.append(p)
    
    # Start all processes
    for p in processes:
        p.start()
    
    # Wait for all processes to finish
    for p in processes:
        p.join()
''' 
    start_time = time.time()
    process_1 = Process(target=get_transit_info, args=(0, 310))
    process_2 = Process(target=get_transit_info, args=(310, 620))
    process_3 = Process(target=get_transit_info, args=(620, 930))
    process_4 = Process(target=get_transit_info, args=(930, 1240))

    process_1.start()
    process_2.start()
    process_3.start()
    process_4.start()

    process_1.join()
    process_2.join()
    process_3.join()
    process_4.join()

    
    print(f"Processing complete in {time.time() - start_time} seconds")
