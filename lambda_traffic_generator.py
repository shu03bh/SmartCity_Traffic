import boto3
import time
import json
import random
from datetime import datetime,timedelta

# AWS Clients
kinesis = boto3.client('kinesis', region_name='us-east-2')
s3 = boto3.client('s3')

stream_name = 'new_stream'
bucket_name = 'smart-city-raw'  # Replace with your bucket name

zones = ['Downtown', 'AirportRoad', 'RingRoad', 'TechPark', 'MallRoad']

def generate_event():
    congestion = round(random.uniform(0.3, 1.0), 2)
    status = 'CRITICAL' if congestion > 0.85 else 'BUSY' if congestion > 0.6 else 'NORMAL'
    return {
        'camera_id': f'CAM{random.randint(1, 5):03}',
        'timestamp': str(datetime.utcnow()+timedelta(minutes=random.randint(1, 180))),
        'zone': random.choice(zones),
        'vehicle_count': random.randint(0, 100),
        'avg_speed': round(random.uniform(20.0, 60.0), 1),
        'congestion_index': congestion,
        'status': status
    }

def stream_and_save(count=5000):
    # Generate all events
    events=[]
    for _ in range(count):
        
        event=generate_event()
        dump_event=json.dumps(event)+'\n'
        put_data=[{'Data': dump_event.encode("utf-8"), 'PartitionKey': event['camera_id']}]
        kinesis.put_records(StreamName=stream_name, Records=put_data)

    # Send to Kinesis as one batch
    # records = [{'Data': json.dumps(e).encode("utf-8"), 'PartitionKey': e['camera_id']} for e in events]
    # res=kinesis.put_records(StreamName=stream_name, Records=records)

    # Save all events in one JSON file in S3 (day-wise folder)
    today = datetime.utcnow()
    folder_path = f"{today.year}/"
    file_name = f"events_{today.month:02}_{today.day:02}_{today.hour:02}_{today.minute:02}.json"
    # s3.put_object(
    #     Bucket=bucket_name,
    #     Key=folder_path + file_name,
    #     Body=json.dumps(events),
    #     ContentType='application/json'
    # )

    return 'success'


def lambda_handler(event, context):
    count = event.get("count", 5000)
    for _ in range (1):
        res=stream_and_save(count=count)


    return {
        "statusCode": 200,
        "body": f"{res}"
    }