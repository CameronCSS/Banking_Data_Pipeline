from faker import Faker
from dotenv import load_dotenv
import os
import pandas as pd
import boto3

# ---- Faker setup ----
fake = Faker()

# ---- Hetzner S3 setup ----
# Load .env file
load_dotenv()

s3 = boto3.resource(
    's3',
    endpoint_url=os.getenv('STORAGE_ENDPOINT'),
    aws_access_key_id=os.getenv('STORAGE_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('STORAGE_SECRET_KEY')
)

bucket_name = os.getenv('STORAGE_BUCKET')
s3_key = 'DataLab/branches/branches.csv'  

# ---- Generate branch data ----
branches = []

for i in range(1, 11):  # 10 Branches
    branches.append({
        "branch_id": i,
        "branch_name": f"{fake.city()} Branch",
        "address": fake.street_address(),
        "city": fake.city(),
        "state": fake.state()
    })

df = pd.DataFrame(branches)

# ---- Save locally (optional) ----
local_file = "branches.csv"
df.to_csv(local_file, index=False)
print("Generated 10 branches locally.")

# ---- Upload to S3 ----
s3.Bucket(bucket_name).upload_file(local_file, s3_key)
print(f"Uploaded branches.csv to s3://{bucket_name}/{s3_key}")
