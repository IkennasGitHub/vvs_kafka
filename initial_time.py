import json
from datetime import datetime
import chardet, pandas
'''# Get the current datetime
current_time = datetime.now()

# Convert the datetime to string format for storage
current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S.%f')

# Store the current datetime string in a file
with open('current_time.json', 'w') as file:
    json.dump({"current_time": current_time_str}, file)

print(f"Current datetime stored in 'current_time.json': {current_time_str}")'''

# Detect the encoding of the file
with open('vvs_steige.csv', 'rb') as file:
    result = chardet.detect(file.read())
    print(result)

# Use the detected encoding to read the file
#df = pandas.read_csv('vvs_steige.csv', delimiter=';', encoding=result['encoding'])
