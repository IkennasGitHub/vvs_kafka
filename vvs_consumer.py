from confluent_kafka import Consumer, KafkaError
import json
#import psycopg2
#from psycopg2.extras import RealDictCursor
from minio import Minio
from minio.error import S3Error
import os
from datetime import datetime

'''conn = psycopg2.connect(
    dbname= "kafka_demo",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

cursor = conn.cursor()'''

# Configure the Kafka consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'vvs_rt_group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['vvs_rt_data'])

# Configure the MinIO client

minio_client = Minio(
    'localhost:9000',
    access_key = 'minioadmin',
    secret_key = 'minioadmin',
    secure = False
)

bucket_name = 'vvs-test-bucket'
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

# Function to process the messages
def process_messages():
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        # Process the message
        data = json.loads(msg.value().decode('utf-8'))
        print(f'Received message: {data}')

        # Insert data into PostgreSQL 
        '''cursor.execute("""
            INSERT INTO train_status (
                station, serving_line, 
                scheduled_time_year, scheduled_time_month, scheduled_time_day,
                real_time,
                delay, cancelled, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data['station'],
            data['serving_line'],
            data['year'], 
            data['month'], 
            data['day'],
            data['time'],
            data['delay'],
            data['cancelled'],
            data['status']))
        
        conn.commit()'''

        # Store in MinIO bucket
        object_name = f"message_{datetime.now().strftime('%d/%m/%Y, %H:%M:%S')}.json"
        object_data = json.dumps(data).encode('utf-8')

        try:
            minio_client.put_object(
                bucket_name,
                object_name,
                data = object_data,
                length = len(object_data),
                content_type = 'application/json'
            )
            print(f'Stored message in MinIO as {object_name}')
        except S3Error as e:
            print(f'Error occured: {e}')
            
# Run the message processing function
process_messages()
