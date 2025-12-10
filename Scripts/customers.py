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

# ---- Load env ----
load_dotenv()
fake = Faker()

# ---- Postgres setup ----
user = os.getenv("PG_USER")
password = quote_plus(os.getenv("PG_PASSWORD"))
host = os.getenv("PG_HOST")
port = "5432"
db = "postgres"
engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

# ---- Hetzner S3 setup ---- (backup only) ----
s3 = boto3.resource(
    "s3",
    endpoint_url=os.getenv("STORAGE_ENDPOINT"),
    aws_access_key_id=os.getenv("STORAGE_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("STORAGE_SECRET_KEY")
)
bucket_name = os.getenv("STORAGE_BUCKET")
branches_s3_key = "DataLab/branches/branches.csv"
customers_s3_key = "DataLab/customers/customers.parquet"

# ---- Load branches from S3 (still needed for customer assignment) ----
branches_local = "../Data/branches.csv"
s3.Bucket(bucket_name).download_file(branches_s3_key, branches_local)
branches = pd.read_csv(branches_local)

# ---- Load existing customers from Postgres for email uniqueness ----
with engine.connect() as conn:
    table_exists = conn.execute(
        text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='customers');")
    ).scalar()

    if table_exists:
        existing_customers = pd.read_sql(
            text("SELECT email FROM customers;"),
            con=conn
        )
        existing_emails = set(existing_customers["email"]) if not existing_customers.empty else set()
    else:
        existing_emails = set()


# ---- Helper functions ----
def realistic_credit_score():
    return max(300, min(int(random.gauss(680, 60)), 850))

def realistic_income():
    brackets = [(20000,40000),(40000,70000),(70000,120000),(120000,200000)]
    low, high = random.choice(brackets)
    return random.randint(low, high)

def realistic_employment():
    return random.choices(
        ["Employed","Self-Employed","Unemployed","Student","Retired"],
        weights=[50,15,10,15,10]
    )[0]

def realistic_contact():
    return random.choice(["Email","Phone","SMS"])

def generate_customer_id():
    return random.getrandbits(48)

# ---- Generate Customers ----
customers = []
for _ in range(50):
    first = fake.first_name()
    last = fake.last_name()
    email = f"{first.lower()}.{last.lower()}@{fake.free_email_domain()}"

    while email in existing_emails:
        first = fake.first_name()
        last = fake.last_name()
        email = f"{first.lower()}.{last.lower()}@{fake.free_email_domain()}"
    existing_emails.add(email)

    dob = fake.date_between(start_date="-80y", end_date="-18y")
    age = (datetime.now().date() - dob).days // 365
    income = realistic_income()
    credit = realistic_credit_score()

    customers.append({
        "customer_id": generate_customer_id(),
        "full_name": f"{first} {last}",
        "email": email,
        "phone": fake.phone_number(),
        "date_of_birth": dob,
        "age": age,
        "gender": random.choice(["Male","Female","Other"]),
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

# ---- Save to S3 backup ----
buffer = io.BytesIO()
df.to_parquet(buffer, index=False, engine="pyarrow")
s3.Bucket(bucket_name).put_object(Key=customers_s3_key, Body=buffer.getvalue())
print("Uploaded customers.parquet to S3 (backup).")

# ---- Insert into Postgres ----
df.to_sql("customers", engine, if_exists="append", index=False, method="multi")
print("Inserted customers into Postgres successfully!")
