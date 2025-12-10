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
accounts_s3_key_parquet = "DataLab/accounts/accounts.parquet"

# ---- Load customers from Postgres ----
with engine.connect() as conn:
    customers_df = pd.read_sql(
        sql=text("SELECT customer_id, home_branch_id, customer_since FROM customers;"),
        con=conn
    )

customers_df["customer_since"] = pd.to_datetime(customers_df["customer_since"]).dt.date

# ---- Unique account ID generator ----
generated_ids = set()
def generate_account_id(branch_id):
    while True:
        branch_part = str(branch_id).zfill(3)
        random_part = str(random.randint(10**8, 10**9 - 1))
        acct_id = branch_part + random_part
        if acct_id not in generated_ids:
            generated_ids.add(acct_id)
            return acct_id

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

# ---- Save to S3 backup ----
buffer = io.BytesIO()
accounts_df.to_parquet(buffer, index=False, engine="pyarrow")
s3.Bucket(bucket_name).put_object(Key=accounts_s3_key_parquet, Body=buffer.getvalue())
print("Uploaded accounts.parquet to S3 (backup).")

# ---- Ensure accounts table exists and insert into Postgres ----
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS accounts (
            account_id      VARCHAR(20) PRIMARY KEY,
            account_number  VARCHAR(20) UNIQUE,
            customer_id     BIGINT REFERENCES customers(customer_id),
            account_type    VARCHAR(50),
            open_date       DATE,
            balance         NUMERIC(12,2),
            branch_id       INT REFERENCES branches(branch_id)
        );
    """))

    # Pandas to_sql now uses the connection from SQLAlchemy 2.x
    accounts_df.to_sql("accounts", conn, if_exists="append", index=False, method="multi")
    print(f"Inserted {len(accounts_df)} accounts into Postgres successfully!")
