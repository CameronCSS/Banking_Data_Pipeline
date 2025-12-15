from faker import Faker
from dotenv import load_dotenv
import os
import json
import boto3
from datetime import datetime, timezone
import uuid

# ---- Setup ----
fake = Faker()
load_dotenv()

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv('STORAGE_ENDPOINT'),
    aws_access_key_id=os.getenv('STORAGE_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('STORAGE_SECRET_KEY')
)

bucket_name = os.getenv('STORAGE_BUCKET')

# Bronze landing zone (RAW)
branches_prefix = "bronze/branches_raw/"

# ---- Generate branch events ----
events = []

now_utc = datetime.now(timezone.utc)

for _ in range(3):
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "branch_created",
        "event_ts": now_utc.isoformat(),
        "branch": {
            "branch_id": str(uuid.uuid4()),
            "branch_name": f"{fake.city()} Branch",
            "address": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "open_date": fake.date_between(start_date="-30d", end_date="today").isoformat(), # New in the last 30 days
            "employee_count": fake.random_int(min=5, max=50),
            "branch_manager": fake.name(),
            "phone_number": fake.phone_number(),
            "timezone": fake.timezone()
        },
        "source_system": "branch_generator",
        "ingestion_ts": now_utc.isoformat()
    }

    events.append(event)

# ---- Write events as JSON lines ----
key = f"{branches_prefix}batch_{now_utc.strftime('%Y%m%d_%H%M%S')}.json"

body = "\n".join(json.dumps(e) for e in events)

s3.put_object(
    Bucket=bucket_name,
    Key=key,
    Body=body.encode("utf-8")
)

print(f"Wrote {len(events)} raw branch events to s3://{bucket_name}/{key}")
