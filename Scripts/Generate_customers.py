from faker import Faker
from dotenv import load_dotenv
import os
import pandas as pd
import boto3
import random
from datetime import datetime

fake = Faker()

# ---- Load env ----
load_dotenv()

# ---- Hetzner S3 setup ----
s3 = boto3.resource(
    "s3",
    endpoint_url=os.getenv("STORAGE_ENDPOINT"),
    aws_access_key_id=os.getenv("STORAGE_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("STORAGE_SECRET_KEY")
)

bucket_name = os.getenv("STORAGE_BUCKET")
customers_s3_key = "DataLab/customers/customers.csv"
branches_s3_key = "DataLab/branches/branches.csv"

# ---- Load branches from S3 ----
branches_local = "../Data/branches.csv"
s3.Bucket(bucket_name).download_file(branches_s3_key, branches_local)
branches = pd.read_csv(branches_local)

# ---- Helper functions ----
def realistic_credit_score():
    """Normal distribution around 680."""
    score = int(random.gauss(680, 60))
    return max(300, min(score, 850))

def realistic_income():
    brackets = [
        (20000, 40000),
        (40000, 70000),
        (70000, 120000),
        (120000, 200000)
    ]
    low, high = random.choice(brackets)
    return random.randint(low, high)

def realistic_employment():
    return random.choices(
        ["Employed", "Self-Employed", "Unemployed", "Student", "Retired"],
        weights=[50, 15, 10, 15, 10]
    )[0]

def realistic_contact():
    return random.choice(["Email", "Phone", "SMS"])

# ---- Generate Customers ----
customers = []
start_id = 100000  # Realistic banking customer IDs

for i in range(50):
    first = fake.first_name()
    last = fake.last_name()
    
    dob = fake.date_between(start_date="-80y", end_date="-18y")
    age = (datetime.now().date() - dob).days // 365

    income = realistic_income()
    credit = realistic_credit_score()

    customers.append({
        "customer_id": start_id + i,
        "first_name": first,
        "last_name": last,
        "full_name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}@{fake.free_email_domain()}",
        "phone": fake.phone_number(),
        "date_of_birth": dob,
        "age": age,
        "gender": random.choice(["Male", "Female", "Other"]),
        "street_address": fake.street_address(),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "zip_code": fake.zipcode(),
        "home_branch_id": random.choice(branches["branch_id"]),
        "customer_since": fake.date_between(start_date="-10y", end_date="today"),
        "employment_status": realistic_employment(),
        "annual_income": income,
        "credit_score": credit,
        "preferred_contact_method": realistic_contact(),
        "is_high_value_customer": income > 120000 or credit > 750,
        "age_group": (
            "18-25" if age < 26 else
            "26-35" if age < 36 else
            "36-50" if age < 51 else
            "51-65" if age < 66 else
            "66+"
        )
    })

df = pd.DataFrame(customers)

# ---- Save locally ----
local_file = "../Data/customers.csv"
df.to_csv(local_file, index=False)
print("Generated realistic customers.")

# ---- Upload to S3 ----
s3.Bucket(bucket_name).upload_file(local_file, customers_s3_key)
print(f"Uploaded customers.csv to s3://{bucket_name}/{customers_s3_key}")
