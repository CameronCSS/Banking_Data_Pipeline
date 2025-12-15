from faker import Faker
from dotenv import load_dotenv
from datetime import datetime, timezone
import os
import json
import boto3
import random
import uuid

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

# Bronze prefixes
accounts_prefix = "bronze/accounts_raw/"
cust_prefix = "bronze/customers_raw/"
branches_prefix = "bronze/branches_raw/"

# ---- Helpers ----
def random_balance():
    return round(random.uniform(-500, 30000), 2)  # overdrafts allowed

def random_account_types():
    roll = random.random()
    if roll < 0.55:
        return ["Checking"]
    elif roll < 0.80:
        return ["Savings"]
    else:
        return ["Checking", "Savings"]

# ---- Load customer IDs from bronze customers ----
cust_ids = set()

resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=cust_prefix)
for obj in resp.get("Contents", []):
    body = s3.get_object(Bucket=bucket_name, Key=obj["Key"])["Body"].read()
    for line in body.decode("utf-8").splitlines():
        record = json.loads(line)
        cust_ids.add(record["customer"]["customer_id"])

if not cust_ids:
    raise ValueError("No customer IDs found in bronze customers data")

# ---- Load existing account customer IDs ----
customers_with_accounts = set()

resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=accounts_prefix)
for obj in resp.get("Contents", []):
    body = s3.get_object(Bucket=bucket_name, Key=obj["Key"])["Body"].read()
    for line in body.decode("utf-8").splitlines():
        record = json.loads(line)
        customers_with_accounts.add(record["customer"]["customer_id"])

# ---- Load branch IDs ----
branch_ids = []

resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=branches_prefix)
for obj in resp.get("Contents", []):
    body = s3.get_object(Bucket=bucket_name, Key=obj["Key"])["Body"].read()
    for line in body.decode("utf-8").splitlines():
        record = json.loads(line)
        branch_ids.append(record["branch"]["branch_id"])

if not branch_ids:
    raise ValueError("No branch IDs found in bronze branches data")

# ---- Determine eligible customers ----
eligible_customers = cust_ids - customers_with_accounts

# ---- Generate ONE account per eligible customer ----
events = []

for cust_id in eligible_customers:
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "account_opened",
        "event_ts": datetime.now(timezone.utc).isoformat(),

        "account": {
            "account_id": str(uuid.uuid4()),
            "account_number": str(random.randint(10**9, 10**11)),
            "account_types": random_account_types(),
            "open_date": fake.date_between(start_date="-30d", end_date="today").isoformat(),
            "balance": random_balance(),
            "currency": random.choice(["USD", "USD", "USD", "EUR"]),
            "interest_rate": round(random.uniform(0.01, 4.5), 2),
            "status": random.choice(["ACTIVE", "ACTIVE", "FROZEN", "CLOSED"]),
        },

        "customer": {
            "customer_id": cust_id,
            "segment": random.choice(["Retail", "SMB", "VIP"]),
        },

        "branch": {
            "branch_id": random.choice(branch_ids),
            "teller_id": random.randint(1000, 9999),
        },

        # intentional noise
        "source_system": "account_generator_v1",
        "batch_id": str(uuid.uuid4()),
        "ingestion_ts": datetime.now(timezone.utc).isoformat(),
    }

    events.append(event)

# ---- Write JSONL batch ----
if events:
    key = f"{accounts_prefix}batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    body = "\n".join(json.dumps(e) for e in events)

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=body.encode("utf-8"),
    )

# ---- Logging (IMPORTANT) ----
print(f"Total customers found: {len(cust_ids)}")
print(f"Customers already with accounts: {len(customers_with_accounts)}")
print(f"New accounts created this run: {len(events)}")
