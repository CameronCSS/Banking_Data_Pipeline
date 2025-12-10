from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from faker import Faker
from dotenv import load_dotenv
import os
import io
import pandas as pd
import boto3
import random
from datetime import datetime
import pyarrow.parquet as pq

user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = "5432"
db = "postgres"

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

fake = Faker()

# ---- Load env ----
load_dotenv()

# ---- Hetzner S3 setup ----
s3 = boto3.resource(
    "s3",
    endpoint_url=os.getenv("STORAGE_ENDPOINT"),
    aws_access_key_id=os.getenv("STORAGE_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("STORAGE_SECRET_KEY")
)

bucket_name = os.getenv("STORAGE_BUCKET")
customers_s3_key = "DataLab/customers/customers.csv"
branches_s3_key = "DataLab/branches/branches.csv"

# ---- Load branches from S3 ----
branches_local = "../Data/branches.csv"
s3.Bucket(bucket_name).download_file(branches_s3_key, branches_local)
branches = pd.read_csv(branches_local)

# ---- Helper functions ----
def realistic_credit_score():
    """Normal distribution around 680."""
    score = int(random.gauss(680, 60))
    return max(300, min(score, 850))

def realistic_income():
    brackets = [
        (20000, 40000),
        (40000, 70000),
        (70000, 120000),
        (120000, 200000)
    ]
    low, high = random.choice(brackets)
    return random.randint(low, high)

def realistic_employment():
    return random.choices(
        ["Employed", "Self-Employed", "Unemployed", "Student", "Retired"],
        weights=[50, 15, 10, 15, 10]
    )[0]

def realistic_contact():
    return random.choice(["Email", "Phone", "SMS"])

# ---- Generate Customers ----
customers = []
start_id = 100000  # Realistic banking customer IDs

for i in range(50):
    first = fake.first_name()
    last = fake.last_name()
    
    dob = fake.date_between(start_date="-80y", end_date="-18y")
    age = (datetime.now().date() - dob).days // 365

    income = realistic_income()
    credit = realistic_credit_score()

    customers.append({
        "customer_id": start_id + i,
        "first_name": first,
        "last_name": last,
        "full_name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}@{fake.free_email_domain()}",
        "phone": fake.phone_number(),
        "date_of_birth": dob,
        "age": age,
        "gender": random.choice(["Male", "Female", "Other"]),
        "street_address": fake.street_address(),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "zip_code": fake.zipcode(),
        "home_branch_id": random.choice(branches["branch_id"]),
        "customer_since": fake.date_between(start_date="-10y", end_date="today"),
        "employment_status": realistic_employment(),
        "annual_income": income,
        "credit_score": credit,
        "preferred_contact_method": realistic_contact()
    })

df = pd.DataFrame(customers)

# ---- Save locally ----
local_file = f"../Data/customers_{datetime.now():%Y%m%d_%H%M%S}.csv"
df.to_csv(local_file, index=False)
print("Generated customers.")


# ---- Upload / append to S3 as Parquet ----
customers_s3_key = "DataLab/customers/customers.parquet"

try:
    # Check if Parquet exists
    obj = s3.Bucket(bucket_name).Object(customers_s3_key).get()
    existing_df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    df_s3 = pd.concat([existing_df, df], ignore_index=True)
    print(f"Appended {len(df_s3)} rows to existing S3 Parquet")
except s3.meta.client.exceptions.NoSuchKey:
    # No existing file
    df_s3 = df
    print("No existing Parquet on S3, creating new one")

# Convert to Parquet buffer
parquet_buffer = io.BytesIO()
df_s3.to_parquet(parquet_buffer, index=False, engine="pyarrow")

# Upload to S3
s3.Bucket(bucket_name).put_object(
    Key=customers_s3_key,
    Body=parquet_buffer.getvalue()
)
print(f"Uploaded customers.parquet to s3")


# ---- Write customers to Postgres ----
try:
    df.to_sql("customers", engine, if_exists="append", index=False)
    print("Inserted customers into Postgres successfully!")
except Exception as e:
    print("Failed to insert into Postgres:", e)


with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM customers;"))
    print(f"Rows in customers table: {result.scalar()}")
