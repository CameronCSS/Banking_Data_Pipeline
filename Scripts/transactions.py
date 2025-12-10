from faker import Faker
from dotenv import load_dotenv
from datetime import datetime
import os
import random
import pandas as pd
import boto3
import io
from sqlalchemy import create_engine, text

# ---- Setup ----
fake = Faker()
load_dotenv()

# ---- Postgres setup ----
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = "5432"
db = "postgres"
engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}", future=True)

# ---- S3 setup (backup only) ----
s3 = boto3.resource(
    "s3",
    endpoint_url=os.getenv("STORAGE_ENDPOINT"),
    aws_access_key_id=os.getenv("STORAGE_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("STORAGE_SECRET_KEY")
)
bucket_name = os.getenv("STORAGE_BUCKET")
transactions_s3_key_parquet = "DataLab/transactions/transactions.parquet"

# ---- Load accounts from Postgres ----
with engine.connect() as conn:
    accounts_df = pd.read_sql(
        sql=text("SELECT account_id, customer_id, branch_id, account_type, open_date, balance FROM accounts;"),
        con=conn
    )

accounts_df["open_date"] = pd.to_datetime(accounts_df["open_date"]).dt.date

# ---- Sample vendors ----
vendors = ["Amazon", "Walmart", "Target", "Starbucks", "Apple", "Netflix", "Uber", "Lyft", "BestBuy", "Costco"]

# ---- Helper functions ----
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
        "branch_id": account['branch_id'] if t_type in ["Deposit", "Withdrawal"] else None,
        "transaction_type": t_type,
        "amount": 0,
        "date": fake.date_between(start_date=pd.to_datetime(account['open_date']), end_date=datetime.today()),
        "balance_after": 0,
        "vendor": random.choice(vendors) if t_type in ["Payment", "Transfer"] else None,
        "transaction_location": None
    }

    if t_type == "Deposit":
        amount = round(random.uniform(20, 10000), 2)
        account['balance'] += amount
        txn["amount"] = amount
        txn["balance_after"] = round(account['balance'], 2)
        txn["transaction_location"] = f"Branch {txn['branch_id']}"
    elif t_type == "Withdrawal":
        amount = round(random.uniform(50, 7000), 2)
        amount = min(amount, account['balance'])
        account['balance'] -= amount
        txn["amount"] = amount
        txn["balance_after"] = round(account['balance'], 2)
        txn["transaction_location"] = f"Branch {txn['branch_id']}"
    else:  # Payment / Transfer
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

# ---- Save to S3 backup ----
buffer = io.BytesIO()
transactions_df.to_parquet(buffer, index=False, engine="pyarrow")
s3.Bucket(bucket_name).put_object(Key=transactions_s3_key_parquet, Body=buffer.getvalue())
print("Uploaded transactions.parquet to S3 (backup).")

# ---- Insert into Postgres ----
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id     VARCHAR(30) PRIMARY KEY,
            account_id         VARCHAR(20) REFERENCES accounts(account_id),
            branch_id          INT,
            transaction_type   VARCHAR(20),
            amount             NUMERIC(12,2),
            date               DATE,
            balance_after      NUMERIC(12,2),
            vendor             VARCHAR(50),
            transaction_location VARCHAR(50)
        );
    """))

    transactions_df.to_sql("transactions", conn, if_exists="append", index=False, method="multi")
    print(f"Inserted {len(transactions_df)} transactions into Postgres successfully!")

# ---- Optional: row count check ----
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM transactions;"))
    print(f"Rows in transactions table: {result.scalar()}")
