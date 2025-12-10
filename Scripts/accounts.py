from faker import Faker
from dotenv import load_dotenv
from datetime import datetime
import os
import random
import pandas as pd
import boto3
import io
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ---- Setup ----
fake = Faker()
load_dotenv()

# ---- S3 Setup ----
s3 = boto3.resource(
    "s3",
    endpoint_url=os.getenv("STORAGE_ENDPOINT"),
    aws_access_key_id=os.getenv("STORAGE_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("STORAGE_SECRET_KEY")
)

bucket_name = os.getenv("STORAGE_BUCKET")
customers_key_csv = "DataLab/customers/customers.csv"
accounts_s3_key_parquet = "DataLab/accounts/accounts.parquet"

# ---- Postgres Setup (optional) ----
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = "5432"
db = "postgres"

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

# ---- Ensure local data folder exists ----
os.makedirs("../Data", exist_ok=True)

# ---- Download customers.csv from S3 ----
local_customers_file = "../Data/customers.csv"
try:
    s3.Bucket(bucket_name).download_file(customers_key_csv, local_customers_file)
    print("Downloaded customers.csv from S3.")
except Exception as e:
    print("ERROR: Could not download customers.csv:", e)
    raise SystemExit()

# ---- Load customers DataFrame ----
customers_df = pd.read_csv(local_customers_file)
customers_df["customer_since"] = pd.to_datetime(customers_df["customer_since"]).dt.date

# ---- Helper Functions ----
def generate_account_id(branch_id):
    branch_part = str(branch_id).zfill(3)
    random_part = str(random.randint(10**8, 10**9 - 1))
    return branch_part + random_part

def generate_account_number():
    return str(random.randint(10**10, 10**11 - 1))

def assign_account_types():
    roll = random.random()
    if roll < 0.50:
        return ["Checking"]
    elif roll < 0.70:
        return ["Savings"]
    else:
        return ["Checking", "Savings"]

def balance_for_type(account_type):
    if account_type == "Checking":
        return round(random.uniform(50, 7000), 2)
    return round(random.uniform(200, 25000), 2)

# ---- Generate accounts ----
accounts = []
for _, row in customers_df.iterrows():
    customer_id = row["customer_id"]
    customer_since = row["customer_since"]
    home_branch_id = row["home_branch_id"]
    account_types = assign_account_types()

    for acct_type in account_types:
        accounts.append({
            "account_id": generate_account_id(home_branch_id),
            "account_number": generate_account_number(),
            "customer_id": customer_id,
            "account_type": acct_type,
            "open_date": fake.date_between(start_date=customer_since, end_date=datetime.today().date()),
            "balance": balance_for_type(acct_type),
            "branch_id": home_branch_id
        })

accounts_df = pd.DataFrame(accounts)

# ---- Save locally as CSV ----
local_accounts_file = "../Data/accounts.csv"
accounts_df.to_csv(local_accounts_file, index=False)
print("Generated accounts.csv locally.")

# ---- Upload / append to S3 as Parquet ----
try:
    obj = s3.Bucket(bucket_name).Object(accounts_s3_key_parquet).get()
    existing_df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    combined_df = pd.concat([existing_df, accounts_df], ignore_index=True)
    print(f"Appended {len(accounts_df)} rows to existing S3 Parquet")
except s3.meta.client.exceptions.NoSuchKey:
    combined_df = accounts_df
    print("No existing Parquet on S3, creating new one")

parquet_buffer = io.BytesIO()
combined_df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
s3.Bucket(bucket_name).put_object(Key=accounts_s3_key_parquet, Body=parquet_buffer.getvalue())
print(f"Uploaded accounts.parquet to s3")

# ---- Append to Postgres ----
try:
    accounts_df.to_sql(
    "accounts",
    engine,
    if_exists="append",
    index=False,
    method='multi'  # faster inserts
)
except Exception as e:
    print("Failed to insert into Postgres:")

# ---- Optional: row count check ----
with engine.connect() as conn:
    existing_ids = pd.read_sql("SELECT account_id FROM accounts;", conn)
    accounts_df = accounts_df[~accounts_df['account_id'].isin(existing_ids['account_id'])]

