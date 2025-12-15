import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

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
spark.stop()

