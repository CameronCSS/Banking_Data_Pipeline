import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

# ---- Windows Hadoop fix (ONLY for winutils) ----
os.environ.setdefault("HADOOP_HOME", "C:\\hadoop")
os.environ["PATH"] += ";C:\\hadoop\\bin"

print("JAVA_HOME =", os.environ.get("JAVA_HOME"))
print("SPARK_HOME =", os.environ.get("SPARK_HOME"))
print("HADOOP_HOME =", os.environ.get("HADOOP_HOME"))

spark = (
    SparkSession.builder
    .appName("bronze-to-silver-test")

    # ---- JARS (single source of truth) ----
    .config(
        "spark.jars.packages",
        ",".join([
            "io.delta:delta-spark_2.12:3.1.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ])
    )

    # ---- Delta ----
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # ---- S3A ----
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("STORAGE_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("STORAGE_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("STORAGE_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    .getOrCreate()
)

print("Spark created OK")
print("fs.s3a.impl =", spark.sparkContext._jsc.hadoopConfiguration().get("fs.s3a.impl"))

df = spark.read.json("s3a://camdoesdata/bronze/transactions_raw/")
df.show(5, truncate=False)

spark.stop()
