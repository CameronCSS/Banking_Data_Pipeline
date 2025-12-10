from faker import Faker
from dotenv import load_dotenv
import os
import pandas as pd
import boto3
import io
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ---- Faker setup ----
fake = Faker()
load_dotenv()

# ---- S3 Setup ----
s3 = boto3.resource(
    's3',
    endpoint_url=os.getenv('STORAGE_ENDPOINT'),
    aws_access_key_id=os.getenv('STORAGE_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('STORAGE_SECRET_KEY')
)

bucket_name = os.getenv('STORAGE_BUCKET')
s3_key_csv = 'DataLab/branches/branches.csv'
s3_key_parquet = 'DataLab/branches/branches.parquet'

# ---- Postgres Setup ----
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = "5432"
db = "postgres"

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

# ---- Ensure local data folder exists ----
os.makedirs("../Data", exist_ok=True)

# ---- Generate branch data ----
branches = []
for i in range(1, 11):  # 10 branches
    branches.append({
        "branch_id": str(i),  # store as string for consistency
        "branch_name": f"{fake.city()} Branch",
        "address": fake.street_address(),
        "city": fake.city(),
        "state": fake.state_abbr()
    })

df = pd.DataFrame(branches)

# ---- Save locally as CSV ----
local_file = "../Data/branches.csv"
df.to_csv(local_file, index=False)
print("Generated 10 branches locally.")

# ---- Upload CSV to S3 ----
s3.Bucket(bucket_name).upload_file(local_file, s3_key_csv)
print(f"Uploaded branches.csv to s3://{bucket_name}/{s3_key_csv}")

# ---- Upload / append to S3 as Parquet ----
try:
    obj = s3.Bucket(bucket_name).Object(s3_key_parquet).get()
    existing_df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    combined_df = pd.concat([existing_df, df], ignore_index=True)
    print(f"Appended {len(df)} branches to existing Parquet on S3.")
except s3.meta.client.exceptions.NoSuchKey:
    combined_df = df
    print("No existing branches Parquet on S3, creating new one.")

parquet_buffer = io.BytesIO()
combined_df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
s3.Bucket(bucket_name).put_object(Key=s3_key_parquet, Body=parquet_buffer.getvalue())
print(f"Uploaded branches.parquet to s3://{bucket_name}/{s3_key_parquet}")

# ---- Create / Append to Postgres ----
with engine.connect() as conn:
    for _, row in df.iterrows():
        stmt = text("""
            INSERT INTO branches (branch_id, branch_name, address, city, state)
            VALUES (:branch_id, :branch_name, :address, :city, :state)
            ON CONFLICT (branch_id) DO NOTHING
        """)
        conn.execute(stmt, {
            "branch_id": str(row["branch_id"]),
            "branch_name": row["branch_name"],
            "address": row["address"],
            "city": row["city"],
            "state": row["state"]
        })
    conn.commit()
    # Optional: row count check
    result = conn.execute(text("SELECT COUNT(*) FROM branches;"))
    print(f"Rows in branches table: {result.scalar()}")
