import boto3          # AWS SDK for Python, lets us talk to AWS services
import time           # Standard library, useful for delays (not used much here)
import json           # For converting Python dicts into JSON strings
import random         # To generate random numbers for synthetic event data
from datetime import datetime, timedelta  # For timestamps

# Fetch Kinesis Stream Name from Secrets Manager

def get_secret(secret_name, region_name="us-east-2"):
    """Retrieve secret from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        # Secrets Manager stores secrets as a JSON string
        secret_dict = json.loads(response["SecretString"])
        return secret_dict.get("stream_name")
    except Exception as e:
        print(f"[ERROR] Unable to retrieve secret: {e}")
        raise

# AWS Clients

region = "us-east-2"
secret_name = "smartcity/kinesis"   # Name of your secret in Secrets Manager

# Get stream name securely
stream_name = get_secret(secret_name, region)

# Create Kinesis client
kinesis = boto3.client("kinesis", region_name=region)

print(f"[INFO] Using Kinesis stream: {stream_name}")
# Zones where cameras are located
zones = ['Downtown', 'AirportRoad', 'RingRoad', 'TechPark', 'MallRoad']

# Function: generate_event

def generate_event():
    # Random congestion index between 0.3 and 1.0
    congestion = round(random.uniform(0.3, 1.0), 2)

    # Status depends on congestion level
    status = 'CRITICAL' if congestion > 0.85 else 'BUSY' if congestion > 0.6 else 'NORMAL'

    # Return a dictionary representing one traffic event
    return {
        'camera_id': f'CAM{random.randint(1, 5):03}',  # Random camera ID like CAM001–CAM005
        'timestamp': str(datetime.utcnow() + timedelta(minutes=random.randint(1, 180))),  # Current UTC time + random offset
        'zone': random.choice(zones),                 # Random zone from the list
        'vehicle_count': random.randint(0, 100),      # Random number of vehicles
        'avg_speed': round(random.uniform(20.0, 60.0), 1),  # Random average speed
        'congestion_index': congestion,               # The congestion index we calculated
        'status': status                              # Status derived from congestion
    }


# Function: stream_and_save

def stream_and_save(count=5000):
    # Loop to generate and send 'count' events
    for _ in range(count):
        event = generate_event()  # Create one synthetic event

        # Convert event to JSON string and add newline
        dump_event = json.dumps(event) + '\n'

        # Prepare record for Kinesis (Data must be bytes, PartitionKey is required)
        put_data = [{'Data': dump_event.encode("utf-8"), 'PartitionKey': event['camera_id']}]

        try:
            # Send record(s) to Kinesis Data Stream
            resp = kinesis.put_records(StreamName=stream_name, Records=put_data)

            # Check if any records failed
            failed = resp.get("FailedRecordCount", 0)
            if failed > 0:
                print(f"[ERROR] Failed to put record: {resp}")
            else:
                print(f"[INFO] Event sent to Kinesis: {event}")
        except ClientError as e:
            # Catch AWS client errors (like permissions or stream not found)
            print(f"[EXCEPTION] Kinesis put_records failed: {e}")

    return 'success'


# Lambda Handler

def lambda_handler(event, context):
    # Get 'count' from the incoming Lambda event, default to 5000
    count = event.get("count", 5000)

    # Call stream_and_save once with the given count
    for _ in range(1):
        res = stream_and_save(count=count)

    # Return a Lambda response object
    return {
        "statusCode": 200,
        "body": f"{res}"
    }
