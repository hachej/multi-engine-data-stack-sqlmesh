# /// script
# dependencies = [
#   "boto3",
#   "pyarrow",
#   "pyiceberg",
#   "polars",
#   "pyiceberg[glue]",
# ]
# ///

import boto3
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Dict
import os
from pyiceberg.catalog import load_catalog
import polars as pl

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

catalog = load_catalog("glue", **{"type": "glue",
                            "glue.region":"eu-central-1",
                            "s3.region":"eu-central-1"
                    })

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
    
    # Set time range - use UTC time
    start_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    end_time = start_time + timedelta(hours=3)
    
    current_time = start_time
    while current_time <= end_time:
        # Generate between 10 and 20 files for this minute
        num_files = random.randint(10, 20)
        
        # Generate the data for this minute
        minute_files_data = generate_minute_data(current_time, num_files)
        
        # Create the partition path using UTC time
        partition_path = current_time.strftime("%Y-%m-%d/%H/%M")
        
        # Save each file
        for file_idx, events in enumerate(minute_files_data):
            file_name = f"events_{file_idx:03d}.jsonl"
            key = f"{PREFIX}{partition_path}/{file_name}"
            save_to_s3(events, BUCKET_NAME, key)

        key_glob = f"s3://{BUCKET_NAME}/{PREFIX}{partition_path}/*"

        print(f"Loading data from {key_glob}")
        df = pl.scan_ndjson(key_glob).collect()
        df = df.with_columns(pl.col("timestamp").cast(pl.Int64).mul(1_000_000).cast(pl.Datetime("us")))
        catalog.load_table("multiengine.events").append(df.to_arrow())
        current_time += timedelta(minutes=1)

if __name__ == "__main__":
    # Set AWS profile
    os.environ['AWS_PROFILE'] = 'admin_aut_prod'
    main()