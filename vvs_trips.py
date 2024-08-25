from vvspy import get_trips  # also usable: get_trips
from vvspy.enums import Station
from datetime import datetime
import json, time
from multiprocessing import Process

file_path = 'debug.jsonl'
station_list = list(Station.__members__.values())

trip_info = {
}

def get_connections(start_index, stop_index):
    for station_index in range(start_index, stop_index):
        try:
            if station_list[station_index].name[0:5] != Station.HAUPTBAHNHOF__TIEF.name[0:5]:
                trips = get_trips(Station.HAUPTBAHNHOF__TIEF, station_list[station_index], check_time= datetime.strptime('2024-08-24 12:00:00.00000', '%Y-%m-%d %H:%M:%S.%f')
                , limit=100)#check_time= datetime.strptime('2024-08-23 02:09:23.898358', '%Y-%m-%d %H:%M:%S.%f')'''
            
                #print(trip)
                #print(f"Duration: {trip.duration / 60} minutes")
                for trip in trips:
                    trip_info['journey'] = f'{trip.connections[0].origin.name} - {trip.connections[-1].destination.name}'
                    trip_info['duration'] = f'{int(trip.duration/60)} minutes'
                    for connection_index in range(len(trip.connections)):

                        trip_info[f'connection_{connection_index+1}'] = {}
                        
                        trip_info[f'connection_{connection_index+1}']['vehicle'] = f'{trip.connections[connection_index].transportation.product["name"]}'
                        trip_info[f'connection_{connection_index+1}']['vehicle_id'] = f'{trip.connections[connection_index].transportation.disassembled_name}'
                        trip_info[f'connection_{connection_index+1}']['origin'] = f'{trip.connections[connection_index].origin.name}'
                        trip_info[f'connection_{connection_index+1}']['departure_time'] = f'{trip.connections[connection_index].origin.departure_time_estimated.strftime("%H:%M")}'
                        trip_info[f'connection_{connection_index+1}']['destination'] = f'{trip.connections[connection_index].destination.name}'
                        trip_info[f'connection_{connection_index+1}']['arrival_time'] = f'{trip.connections[connection_index].destination.arrival_time_estimated.strftime("%H:%M")}'
                        
                        '''print(f'\n{trip.connections[0].origin.name} - {trip.connections[-1].destination.name}')
                        print(f'\n{trip}')
                        print(f'\n{trip.connections[connection_index].transportation.disassembled_name}')
                        print(f'\n{int(trip.duration/60)} minutes')
                        print(f'\n{trip.connections[connection_index].origin.departure_time_estimated.strftime("%H:%M")}')
                        print(f'\n{trip.connections[connection_index].origin.name}')
                        print(f'\n{trip.connections[connection_index].destination.name}')
                        print(f'\n{trip.connections[connection_index].destination.arrival_time_estimated.strftime("%H:%M")}')
                        print(f'\n{trip.connections[connection_index].transportation.product["name"]}')'''

                    with open(file_path, 'a', encoding='utf-8') as f:
                        f.write(json.dumps(trip_info, ensure_ascii=False)+ '\n')
                        f.flush()
                        #print('Data written successfully.')
            else:
                continue        
        except Exception as e:
            print(f'An error occured: {e} at index {station_list[station_index]}: {station_list[station_index].name}')
            continue


if __name__ == '__main__':
        
        start_time = time.time()
        process_1 = Process(target=get_connections, args=(0, 5000))
        process_2 = Process(target=get_connections, args=(5000, 10000))
        process_3 = Process(target=get_connections, args=(10000, 15000))
        process_4 = Process(target=get_connections, args=(15000, 19933  ))

        process_1.start()
        process_2.start()
        process_3.start()
        process_4.start()

        print('Running...')

        process_1.join()
        process_2.join()
        process_3.join()
        process_4.join()
        
        print(f"Processing complete in {(time.time() - start_time)/60} minutes")