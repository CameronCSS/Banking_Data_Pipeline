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
accounts_key = "DataLab/accounts/accounts.csv"
transactions_s3_key = "DataLab/transactions/transactions.csv"

# ---- Ensure local data folder exists ----
os.makedirs("../Data", exist_ok=True)

# ---- Download accounts.csv from S3 ----
local_accounts_file = "../Data/accounts.csv"
try:
    s3.Bucket(bucket_name).download_file(accounts_key, local_accounts_file)
    print("Downloaded accounts.csv from S3.")
except Exception as e:
    print("ERROR: Could not download accounts.csv:", e)
    raise SystemExit()

# ---- Load accounts DataFrame ----
accounts_df = pd.read_csv(local_accounts_file)

# ---- Sample vendors ----
vendors = ["Amazon", "Walmart", "Target", "Starbucks", "Apple", "Netflix", "Uber", "Lyft", "BestBuy", "Costco"]

# ---- Helper Functions ----
def generate_transaction_id(account_id, idx):
    """Generate a unique transaction ID combining account ID and index."""
    return f"{account_id}{str(idx).zfill(5)}"

def generate_transaction(account):
    """Generate a realistic transaction for a given account."""
    t_type = random.choices(
        ["Deposit", "Withdrawal", "Payment", "Transfer"], 
        weights=[0.4, 0.3, 0.2, 0.1], k=1
    )[0]

    transaction_data = {
        "transaction_id": None,  # fill later
        "account_id": account['account_id'],
        "branch_id": None,
        "transaction_type": t_type,
        "amount": 0,
        "date": fake.date_between(start_date=pd.to_datetime(account['open_date']), end_date=datetime.today()),
        "balance_after": 0,
        "vendor": None,
        "transaction_location": None
    }

    if t_type in ["Deposit", "Withdrawal"]:
        # Pick one of the branches for deposit/withdrawal
        transaction_data["branch_id"] = account['branch_id']
        amount = round(random.uniform(50, 7000), 2) if t_type == "Withdrawal" else round(random.uniform(20, 10000), 2)
        if t_type == "Withdrawal":
            amount = min(amount, account['balance'])
            account['balance'] -= amount
        else:
            account['balance'] += amount
        transaction_data["amount"] = amount
        transaction_data["balance_after"] = round(account['balance'], 2)
        transaction_data["transaction_location"] = f"Branch {account['branch_id']}"

    else:  # Payment or Transfer
        transaction_data["branch_id"] = None
        transaction_data["vendor"] = random.choice(vendors)
        amount = round(random.uniform(5, 1000), 2)
        account['balance'] = max(account['balance'] - amount, 0)
        transaction_data["amount"] = amount
        transaction_data["balance_after"] = round(account['balance'], 2)
        transaction_data["transaction_location"] = "POS / Online"

    return transaction_data

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

# ---- Convert to DataFrame ----
transactions_df = pd.DataFrame(transactions)

# ---- Save locally ----
local_transactions_file = "../Data/transactions.csv"
transactions_df.to_csv(local_transactions_file, index=False)
print("Generated transactions.csv locally with realistic branch/vendor data.")

# ---- Upload to S3 ----
try:
    s3.Bucket(bucket_name).upload_file(local_transactions_file, transactions_s3_key)
    print(f"Uploaded transactions.csv to s3://{bucket_name}/{transactions_s3_key}")
except Exception as e:
    print("ERROR: Could not upload transactions.csv to S3:", e)
