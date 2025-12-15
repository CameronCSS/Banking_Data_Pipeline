from faker import Faker
from dotenv import load_dotenv
import os
import json
import boto3
import uuid
import random
from datetime import datetime, timezone

# ---- Setup ----
fake = Faker()
load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("STORAGE_ENDPOINT"),
    aws_access_key_id=os.getenv("STORAGE_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("STORAGE_SECRET_KEY")
)

bucket_name = os.getenv("STORAGE_BUCKET")

# Bronze landing zone
cust_prefix = "bronze/customers_raw/"
branches_prefix = "bronze/branches_raw/"

# ---- Helper generators (intentionally imperfect) ----
def random_credit_score():
    return random.randint(250, 900)  # invalid values on purpose

def random_income():
    return random.choice([
        random.randint(15000, 30000),
        random.randint(30000, 80000),
        random.randint(80000, 200000),
        None
    ])

def random_employment():
    return random.choice([
        "Employed",
        "Self-Employed",
        "Unemployed",
        "Student",
        "Retired",
        "Unknown",
        None
    ])
    
# ---- Load branch IDs from bronze ----
branch_ids = []

response = s3.list_objects_v2(Bucket=bucket_name, Prefix=branches_prefix)
for obj in response.get("Contents", []):
    body = s3.get_object(Bucket=bucket_name, Key=obj["Key"])["Body"].read()
    for line in body.decode("utf-8").splitlines():
        record = json.loads(line)
        branch_ids.append(record["branch"]["branch_id"])

if not branch_ids:
    raise ValueError("No branch IDs found in bronze branches data")


# ---- Generate customer events ----
events = []

for _ in range(150):
    dob = fake.date_between(start_date="-90y", end_date="-16y")

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(["customer_created", "customer_updated"]),
        "event_ts": datetime.now(timezone.utc).isoformat(),

        "customer": {
            "customer_id": random.getrandbits(48),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),  # duplicates allowed
            "phone": fake.phone_number(),
            "date_of_birth": dob.isoformat(),
            "gender": random.choice(["Male", "Female", "Other", None]),
            "married": random.choice([True, False, "Unknown"]),
            "employment_status": random_employment(),
            "annual_income": random_income(),
            "credit_score": random_credit_score(),
            "home_branch_id": random.choice(branch_ids),
            "customer_since": fake.date_between(start_date="-30d", end_date="today").isoformat(), # New in the last 30 days
            "preferred_contact_method": random.choice(
                ["Email", "Phone", "SMS", "Mail", None]
            ),
            # extra junk fields
            "browser": fake.user_agent(),
            "ip_address": fake.ipv4_public(),
            "marketing_opt_in": random.choice([True, False, None])
        },

        "source_system": "customer_generator",
        "ingestion_ts": datetime.now(timezone.utc).isoformat()
    }

    events.append(event)

# ---- Write JSON lines to S3 ----
key = f"{cust_prefix}batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"

body = "\n".join(json.dumps(e) for e in events)

s3.put_object(
    Bucket=bucket_name,
    Key=key,
    Body=body.encode("utf-8")
)

print(f"Wrote {len(events)} raw customer events to s3://{bucket_name}/{key}")
