from faker import Faker
from dotenv import load_dotenv
from datetime import datetime
import os
import random
import pandas as pd
import boto3

# ---- Setup ----
fake = Faker()
load_dotenv()

s3 = boto3.resource(
    "s3",
    endpoint_url=os.getenv("STORAGE_ENDPOINT"),
    aws_access_key_id=os.getenv("STORAGE_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("STORAGE_SECRET_KEY")
)

bucket_name = os.getenv("STORAGE_BUCKET")
customers_key = "DataLab/customers/customers.csv"
accounts_s3_key = "DataLab/accounts/accounts.csv"

# ---- Ensure local data folder exists ----
os.makedirs("../Data", exist_ok=True)

# ---- Download customers.csv from S3 ----
local_customers_file = "../Data/customers.csv"
try:
    s3.Bucket(bucket_name).download_file(customers_key, local_customers_file)
    print("Downloaded customers.csv from S3.")
except Exception as e:
    print("ERROR: Could not download customers.csv:", e)
    raise SystemExit()

# ---- Load customers DataFrame ----
customers_df = pd.read_csv(local_customers_file)

# Convert customer_since to actual date objects
customers_df["customer_since"] = pd.to_datetime(customers_df["customer_since"]).dt.date

# ---- Helper Functions ----

def generate_account_id(branch_id):
    """Generate realistic branch-coded account IDs (11–12 digits)."""
    branch_part = str(branch_id).zfill(3)  # 3-digit branch ID
    random_part = str(random.randint(10**8, 10**9 - 1))  # 8–9 random digits
    return branch_part + random_part

def generate_account_number():
    """Generate realistic 11-digit bank account numbers."""
    return str(random.randint(10**10, (10**11) - 1))

def assign_account_types():
    """
    Assign 1–2 accounts per customer using realistic rules:
      - ~50% Checking Only
      - ~20% Savings Only
      - ~30% Both
    """
    roll = random.random()

    if roll < 0.50:
        return ["Checking"]
    elif roll < 0.70:
        return ["Savings"]
    else:
        return ["Checking", "Savings"]

def balance_for_type(account_type):
    """Give realistic account balances."""
    if account_type == "Checking":
        return round(random.uniform(50, 7000), 2)
    else:  # Savings
        return round(random.uniform(200, 25000), 2)

# ---- Generate accounts ----
accounts = []

for _, row in customers_df.iterrows():
    customer_id = row["customer_id"]
    customer_since = row["customer_since"]
    home_branch_id = row["home_branch_id"]

    # Determine which account types this customer owns
    account_types = assign_account_types()

    for acct_type in account_types:
        accounts.append({
            "account_id": generate_account_id(home_branch_id),
            "account_number": generate_account_number(),
            "customer_id": customer_id,
            "account_type": acct_type,
            "open_date": fake.date_between(
                start_date=customer_since, end_date=datetime.today().date()
            ),
            "balance": balance_for_type(acct_type),
            "branch_id": home_branch_id
        })

# ---- Convert to DataFrame ----
accounts_df = pd.DataFrame(accounts)

# ---- Save locally ----
local_accounts_file = "../Data/accounts.csv"
accounts_df.to_csv(local_accounts_file, index=False)
print("Generated accounts.csv locally.")

# ---- Upload to S3 ----
try:
    s3.Bucket(bucket_name).upload_file(local_accounts_file, accounts_s3_key)
    print(f"Uploaded accounts.csv to s3://{bucket_name}/{accounts_s3_key}")
except Exception as e:
    print("ERROR: Could not upload accounts.csv to S3:", e)
