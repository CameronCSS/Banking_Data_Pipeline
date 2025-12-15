from faker import Faker
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
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

# Bronze locations
transactions_prefix = "bronze/transactions_raw/"
accounts_prefix = "bronze/accounts_raw/"

# ---- Load account IDs from bronze ----
account_ids = []

resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=accounts_prefix)
for obj in resp.get("Contents", []):
    body = s3.get_object(Bucket=bucket_name, Key=obj["Key"])["Body"].read()
    for line in body.decode("utf-8").splitlines():
        record = json.loads(line)
        account_ids.append(record["account"]["account_id"])

if not account_ids:
    raise ValueError("No account IDs found in bronze accounts data")


MERCHANTS_BY_CATEGORY = {
    "Grocery": [
        "Walmart", "Target", "Costco", "Kroger", "Safeway",
        "Publix", "Whole Foods", "Trader Joe's", "Aldi"
    ],
    "Coffee": ["Starbucks", "Dunkin'"],
    "Fast Food": [
        "McDonald's", "Burger King", "Wendy's",
        "Chick-fil-A", "Taco Bell", "KFC"
    ],
    "Dining": [
        "Chipotle", "Panera Bread", "Olive Garden",
        "Applebee's", "Chili's"
    ],
    "Online Shopping": ["Amazon"],
    "Electronics": ["Best Buy"],
    "Home Improvement": ["Home Depot", "Lowe's"],
    "Furniture": ["IKEA"],
    "Fuel": ["Shell", "Exxon", "Chevron", "BP"],
    "Convenience Store": ["7-Eleven", "Circle K", "Wawa"],
    "Ride Share": ["Uber", "Lyft"],
    "Public Transit": ["Amtrak"],
    "Car Rental": ["Enterprise Rent-A-Car", "Hertz"],
    "Airfare": [
        "Delta Airlines", "United Airlines",
        "American Airlines", "Southwest Airlines"
    ],
    "Streaming Services": [
        "Netflix", "Hulu", "Disney+",
        "Spotify", "Apple Music"
    ],
    "Movies": ["AMC Theatres", "Regal Cinemas"],
    "Pharmacy": ["CVS", "Walgreens", "Rite Aid"],
    "Healthcare": ["Kaiser Permanente"],
    "Internet": ["Comcast", "Spectrum"],
    "Mobile Phone": ["AT&T", "Verizon", "T-Mobile"],
    "Bank Fee": ["Chase", "Bank of America"],
}

# Flatten for fast lookup
MERCHANT_CATEGORY_MAP = {
    merchant: category
    for category, merchants in MERCHANTS_BY_CATEGORY.items()
    for merchant in merchants
}

VENDORS = list(MERCHANT_CATEGORY_MAP.keys())
CATEGORIES = list(MERCHANTS_BY_CATEGORY.keys())

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def get_category_for_vendor(vendor: str, noise_rate: float = 0.05) -> str:
    """
    Deterministic category lookup with optional noise.
    """
    if random.random() < noise_rate:
        return random.choice(CATEGORIES)
    return MERCHANT_CATEGORY_MAP[vendor]

def pick_transaction_context():
    """
    Realistic transaction type → merchant pairing
    """
    txn_type = random.choice(["PURCHASE", "WITHDRAWAL", "DEPOSIT", "TRANSFER"])

    if txn_type == "WITHDRAWAL":
        return txn_type, "ATM Withdrawal", "Cash Withdrawal"
    if txn_type == "DEPOSIT":
        return txn_type, "Direct Deposit", "Income"
    if txn_type == "TRANSFER":
        return txn_type, "Account Transfer", "Transfer"

    merchant = random.choice(VENDORS)
    return txn_type, merchant, get_category_for_vendor(merchant)

# -------------------------------------------------------------------
# Static enums
# -------------------------------------------------------------------

channels = ["POS", "ONLINE", "ATM"]
statuses = ["POSTED", "PENDING", "DECLINED"]
currencies = ["USD", "USD", "USD", "EUR", "CAD"]

# -------------------------------------------------------------------
# Generate transaction events
# -------------------------------------------------------------------

events = []
NUM_TRANSACTIONS = 2000

for _ in range(NUM_TRANSACTIONS):
    account_id = random.choice(account_ids)
    txn_type, merchant, category = pick_transaction_context()

    txn_ts = datetime.now(timezone.utc) - timedelta(
        days=random.randint(0, 365),
        minutes=random.randint(0, 1440),
    )

    amount = round(random.uniform(1.00, 2500.00), 2)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "transaction_posted",
        "event_ts": txn_ts.isoformat(),

        "transaction": {
            "transaction_id": str(uuid.uuid4()),
            "account_id": account_id,
            "amount": amount,
            "currency": random.choice(currencies),
            "transaction_type": txn_type,
            "merchant_name": merchant,
            "category": category,
            "merchant_id": str(uuid.uuid4()),
            "merchant_city": fake.city(),
            "merchant_state": fake.state_abbr(),
            "merchant_country": "US",
            "merchant_mcc": random.randint(1000, 9999),
            "channel": random.choice(channels),
            "status": random.choice(statuses),
            "is_international": random.choice([True, False]),
            "is_recurring": merchant in {
                "Netflix", "Spotify", "Comcast", "AT&T"
            },
            "auth_code": random.randint(100000, 999999),
            "device_id": str(uuid.uuid4()),
            "ip_address": fake.ipv4_public(),
        },

        "source_system": "transaction_generator_v1",
        "batch_id": str(uuid.uuid4()),
        "ingestion_ts": datetime.now(timezone.utc).isoformat(),
    }

    events.append(event)

# -------------------------------------------------------------------
# Write JSONL to S3
# -------------------------------------------------------------------

key = (
    f"{transactions_prefix}"
    f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
)

body = "\n".join(json.dumps(e) for e in events)

s3.put_object(
    Bucket=bucket_name,
    Key=key,
    Body=body.encode("utf-8"),
)

print(f"Wrote {len(events)} raw transaction events to s3://{bucket_name}/{key}")
