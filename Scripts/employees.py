from faker import Faker
from dotenv import load_dotenv
import os
import json
import boto3
from datetime import datetime, timezone
import uuid
import random

# ---- Setup ----
fake = Faker()
load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("STORAGE_ENDPOINT"),
    aws_access_key_id=os.getenv("STORAGE_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("STORAGE_SECRET_KEY"),
)

bucket_name = os.getenv("STORAGE_BUCKET")

branches_prefix = "bronze/branches_raw/"
employees_prefix = "bronze/employees_raw/"

# ------------------------------------------------
# Load branch IDs
# ------------------------------------------------
branch_ids = []

resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=branches_prefix)
for obj in resp.get("Contents", []):
    body = s3.get_object(Bucket=bucket_name, Key=obj["Key"])["Body"].read()
    for line in body.decode("utf-8").splitlines():
        record = json.loads(line)
        branch_ids.append(record["branch"]["branch_id"])

if not branch_ids:
    raise ValueError("No branch IDs found")

# ------------------------------------------------
# Load existing employees from bronze
# ------------------------------------------------
existing_employee_ids = []

resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=employees_prefix)
for obj in resp.get("Contents", []):
    body = s3.get_object(Bucket=bucket_name, Key=obj["Key"])["Body"].read()
    for line in body.decode("utf-8").splitlines():
        record = json.loads(line)
        if "employee" in record:
            existing_employee_ids.append(record["employee"]["employee_id"])

existing_employee_ids = list(set(existing_employee_ids))

# ------------------------------------------------
# Event generation config
# ------------------------------------------------
NEW_EMPLOYEES = 60
TERMINATIONS = min(len(existing_employee_ids), random.randint(10, 30))

events = []

# ------------------------------------------------
# Create new employees
# ------------------------------------------------
for _ in range(NEW_EMPLOYEES):
    birth_date = fake.date_between(start_date="-65y", end_date="-18y")

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "employee_created",
        "event_ts": datetime.now(timezone.utc).isoformat(),

        "employee": {
            "employee_id": str(uuid.uuid4()),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "birth_date": birth_date.isoformat(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "married": random.choice([True, False, None]),
            "job_title": fake.job(),
            "salary": random.randint(35000, 140000),
            "work_satisfaction": random.randint(1, 5),
            "hire_date": fake.date_between(start_date="-30d", end_date="today").isoformat(),
            "employment_type": random.choice(["full_time", "part_time", "contract"]),
            "remote": fake.boolean(),
            "branch_id": random.choice(branch_ids)
        },

        "source_system": "employee_generator",
        "ingestion_ts": datetime.now(timezone.utc).isoformat()
    }

    events.append(event)

# ------------------------------------------------
# Terminate existing employees
# ------------------------------------------------
terminated_ids = random.sample(existing_employee_ids, TERMINATIONS)

for emp_id in terminated_ids:
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "employee_terminated",
        "event_ts": datetime.now(timezone.utc).isoformat(),

        "employee": {
            "employee_id": emp_id,
            "termination_reason": random.choice(
                ["Resigned", "Laid Off", "Retired", "Fired"]
            )
        },

        "source_system": "employee_generator",
        "ingestion_ts": datetime.now(timezone.utc).isoformat()
    }

    events.append(event)

# ------------------------------------------------
# Write to S3 (JSONL)
# ------------------------------------------------
key = f"{employees_prefix}batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
body = "\n".join(json.dumps(e) for e in events)

s3.put_object(
    Bucket=bucket_name,
    Key=key,
    Body=body.encode("utf-8")
)

# ------------------------------------------------
# Stats output
# ------------------------------------------------
print(f"Existing employees found: {len(existing_employee_ids)}")
print(f"New employees created: {NEW_EMPLOYEES}")
print(f"Employees terminated this run: {len(terminated_ids)}")
print(f"{len(events)} events written to s3://{bucket_name}/{key}")
