from faker import Faker
import pandas as pd
import boto3

# ---- Faker setup ----
fake = Faker()

# ---- Hetzner S3 setup ----
s3 = boto3.resource(
    's3',
    endpoint_url='https://fsn1.your-objectstorage.com',
    aws_access_key_id='LXH38VQ3K0A87TZS0KXP',                 
    aws_secret_access_key='tkJt2iNjmEj1KAqtx2Tvb3WqQNkOxgqJzHC7Iq1H'             
)

bucket_name = 'CamDoesData'
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
# local_file = "branches.csv"
# df.to_csv(local_file, index=False)
# print("Generated 10 branches locally.")

# ---- Upload to S3 ----
s3.Bucket(bucket_name).upload_file(local_file, s3_key)
print(f"Uploaded branches.csv to s3://{bucket_name}/{s3_key}")
