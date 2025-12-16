import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql import functions as F

load_dotenv()

# ---- WINDOWS FIX ----
os.environ.setdefault("HADOOP_HOME", "C:\\hadoop")
os.environ.setdefault("hadoop.home.dir", "C:\\hadoop")
os.environ["PATH"] += ";C:\\hadoop\\bin"


spark = (
    SparkSession.builder
    .appName("bronze-to-silver-batch")

    # ---- ALL JARS IN ONE PLACE ----
    .config(
        "spark.jars.packages",
        ",".join([
            # Delta
            "io.delta:delta-core_2.12:2.3.0",

            # S3A
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ])
    )

    # ---- DELTA ----
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # ---- S3 ----
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("STORAGE_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("STORAGE_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("STORAGE_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    .getOrCreate()
    
)
    
print("Spark created OK")


# Prove S3A filesystem class is on the classpath
print("fs.s3a.impl =", spark.sparkContext._jsc.hadoopConfiguration().get("fs.s3a.impl"))

# Force a real read/action from S3
df = spark.read.json("s3a://camdoesdata/bronze/transactions_raw/")
print("About to show() ...")
df.limit(5).show(truncate=False)

print("Done.")


# ---- READ TRANSACTIONS ----
transactions_df = spark.read.json("s3a://camdoesdata/bronze/transactions_raw/")

# ---- METHOD 1: 5 random transaction records ----
random_txns = (
    transactions_df
    .select(
        F.col("transaction.account_id").alias("account_id"),
        F.col("transaction.amount").alias("amount"),
        F.col("transaction.merchant_name").alias("merchant"),
        F.col("transaction.category").alias("category")
    )
    .sample(fraction=0.1)
    .limit(5)
)

print("5 Random Transactions:")
random_txns.show(truncate=False)

# ---- METHOD 2: All transactions for 5 random accounts ----
# Get 5 random account IDs
random_account_ids = (
    transactions_df
    .select(F.col("transaction.account_id").alias("account_id"))
    .distinct()
    .orderBy(F.rand())  # Random shuffle
    .limit(5)
)

# Get all their transactions
all_txns_for_random_accounts = (
    transactions_df
    .select(
        F.col("transaction.account_id").alias("account_id"),
        F.col("transaction.amount").alias("amount"),
        F.col("transaction.merchant_name").alias("merchant"),
        F.col("transaction.transaction_type").alias("type"),
        F.col("event_ts").alias("timestamp")
    )
    .join(random_account_ids, on="account_id", how="inner")
    .orderBy("account_id", "timestamp")
)

print("\nAll Transactions for 5 Random Accounts:")
all_txns_for_random_accounts.show(50, truncate=False)

# ---- METHOD 3: Summary per account (cleaner view) ----
summary = (
    transactions_df
    .select(
        F.col("transaction.account_id").alias("account_id"),
        F.col("transaction.amount").alias("amount")
    )
    .join(random_account_ids, on="account_id", how="inner")
    .groupBy("account_id")
    .agg(
        F.count("*").alias("num_transactions"),
        F.collect_list("amount").alias("amounts"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount")
    )
)

print("\nSummary of 5 Random Accounts:")
summary.show(truncate=False)

spark.stop()

