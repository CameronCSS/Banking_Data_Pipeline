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
accounts_s3_key_parquet = "DataLab/accounts/accounts.parquet"
transactions_s3_key_parquet = "DataLab/transactions/transactions.parquet"

# ---- Postgres Setup ----
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = quote_plus(os.getenv("PG_PASSWORD"))
PG_HOST = os.getenv("PG_HOST")
PG_PORT = "5432"
PG_DB = "postgres"

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# ---- Ensure local data folder exists ----
os.makedirs("../Data", exist_ok=True)

# ---- Download accounts parquet from S3 ----
accounts_df = pd.DataFrame()
try:
    obj = s3.Bucket(bucket_name).Object(accounts_s3_key_parquet).get()
    accounts_df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    print("Downloaded accounts.parquet from S3.")
except s3.meta.client.exceptions.NoSuchKey:
    print("ERROR: Accounts Parquet not found on S3.")
    raise SystemExit()

# ---- Sample vendors ----
vendors = ["Amazon", "Walmart", "Target", "Starbucks", "Apple", "Netflix", "Uber", "Lyft", "BestBuy", "Costco"]

# ---- Helper Functions ----
def generate_transaction_id(account_id, idx):
    return f"{account_id}{str(idx).zfill(5)}"

def generate_transaction(account):
    t_type = random.choices(
        ["Deposit", "Withdrawal", "Payment", "Transfer"],
        weights=[0.4, 0.3, 0.2, 0.1], k=1
    )[0]

    txn = {
        "transaction_id": None,  # to be filled later
        "account_id": str(account['account_id']),
        "branch_id": None,
        "transaction_type": t_type,
        "amount": 0,
        "date": fake.date_between(start_date=pd.to_datetime(account['open_date']), end_date=datetime.today()),
        "balance_after": 0,
        "vendor": None,
        "transaction_location": None
    }

    if t_type in ["Deposit", "Withdrawal"]:
        txn["branch_id"] = account.get('branch_id', None)
        amount = round(random.uniform(50, 7000), 2) if t_type == "Withdrawal" else round(random.uniform(20, 10000), 2)
        if t_type == "Withdrawal":
            amount = min(amount, account['balance'])
            account['balance'] -= amount
        else:
            account['balance'] += amount
        txn["amount"] = amount
        txn["balance_after"] = round(account['balance'], 2)
        txn["transaction_location"] = f"Branch {txn['branch_id']}"
    else:  # Payment / Transfer
        txn["vendor"] = random.choice(vendors)
        amount = round(random.uniform(5, 1000), 2)
        account['balance'] = max(account['balance'] - amount, 0)
        txn["amount"] = amount
        txn["balance_after"] = round(account['balance'], 2)
        txn["transaction_location"] = "POS / Online"

    return txn

# ---- Generate transactions ----
transactions = []
idx = 1

for _, account in accounts_df.iterrows():
    account_transactions_count = random.randint(5, 20)
    for _ in range(account_transactions_count):
        txn = generate_transaction(account)
        txn['transaction_id'] = generate_transaction_id(account['account_id'], idx)
        transactions.append(txn)
        idx += 1

transactions_df = pd.DataFrame(transactions)

# ---- Upload / append to S3 as Parquet ----
try:
    obj = s3.Bucket(bucket_name).Object(transactions_s3_key_parquet).get()
    existing_df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    combined_df = pd.concat([existing_df, transactions_df], ignore_index=True)
    print(f"Appended {len(transactions_df)} transactions to existing Parquet on S3.")
except s3.meta.client.exceptions.NoSuchKey:
    combined_df = transactions_df
    print("No existing transactions Parquet on S3, creating new one.")

# Convert to Parquet and upload
parquet_buffer = io.BytesIO()
combined_df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
s3.Bucket(bucket_name).put_object(Key=transactions_s3_key_parquet, Body=parquet_buffer.getvalue())
print(f"Uploaded combined transactions.parquet to s3://{bucket_name}/{transactions_s3_key_parquet}")

# ---- Append to Postgres ----
try:
    combined_df.to_sql("transactions", engine, if_exists="append", index=False)
    print("Inserted transactions into Postgres successfully!")
except Exception as e:
    print("Failed to insert into Postgres:", e)

# ---- Optional: row count check ----
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM transactions;"))
    print(f"Rows in transactions table: {result.scalar()}")
