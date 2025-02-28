# /// script
# dependencies = [
#   "boto3",
#   "pyarrow",
# ]
# ///

import boto3
import json
import random
import uuid
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Dict
import os

# Website actions that users might perform
POSSIBLE_ACTIONS = [
    "page_view",
    "click",
    "scroll",
    "form_submit",
    "search",
    "add_to_cart",
    "checkout",
    "login",
    "logout",
    "video_play",
    "video_pause"
]

def generate_event(timestamp: datetime) -> Dict:
    """Generate a single event with the specified schema"""
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4())[:8],  # Shorter UUID for user_id
        "action": random.choice(POSSIBLE_ACTIONS),
        "timestamp": int(timestamp.timestamp())
    }

def generate_minute_data(dt: datetime, num_files: int) -> List[List[Dict]]:
    """Generate data for a specific minute, split into multiple files"""
    events_per_file = random.randint(50, 200)  # Random number of events per file
    all_files_data = []
    
    for _ in range(num_files):
        minute_events = []
        for _ in range(events_per_file):
            # Generate event with timestamp within this minute
            event_timestamp = dt + timedelta(seconds=random.randint(0, 59))
            minute_events.append(generate_event(event_timestamp))
        all_files_data.append(minute_events)
    
    return all_files_data

def save_to_s3(events: List[Dict], bucket: str, key: str) -> None:
    """Save events as JSONL to S3"""
    s3 = boto3.client('s3')
    jsonl_content = '\n'.join(json.dumps(event) for event in events)
    s3.put_object(Bucket=bucket, Key=key, Body=jsonl_content.encode('utf-8'))

def main():
    BUCKET_NAME = "sumeo-parquet-data-lake"
    PREFIX = "multiengine/row/"
    
    # Set time range
    start_time = datetime.now().replace(second=0, microsecond=0)
    end_time = start_time + timedelta(hours=3)
    
    current_time = start_time
    while current_time <= end_time:
        # Generate between 10 and 20 files for this minute
        num_files = random.randint(10, 20)
        
        # Generate the data for this minute
        minute_files_data = generate_minute_data(current_time, num_files)
        
        # Create the partition path
        partition_path = current_time.strftime("%Y-%m-%d/%H/%M")
        
        # Save each file
        for file_idx, events in enumerate(minute_files_data):
            file_name = f"events_{file_idx:03d}.jsonl"
            key = f"{PREFIX}{partition_path}/{file_name}"
            save_to_s3(events, BUCKET_NAME, key)
            
            print(f"Saved file: {key}")
        
        current_time += timedelta(minutes=1)

if __name__ == "__main__":
    # Set AWS profile
    os.environ['AWS_PROFILE'] = 'admin_aut_prod'
    main()