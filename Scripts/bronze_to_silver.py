import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession


load_dotenv()

# ---- SET JAVA_HOME using short path (no spaces) ----
os.environ["JAVA_HOME"] = "C:\\PROGRA~1\\ECLIPS~1\\JDK-17~1.10-"

# ---- SET PYSPARK_PYTHON to current Python executable ----
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Set hostname to avoid networking issues
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

print(f"JAVA_HOME: {os.environ['JAVA_HOME']}")
print(f"PYSPARK_PYTHON: {os.environ['PYSPARK_PYTHON']}")

# ---- WINDOWS FIX ----
HADOOP_HOME = "C:\\hadoop"
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["hadoop.home.dir"] = HADOOP_HOME

system32 = "C:\\Windows\\System32"
os.environ["PATH"] = f"{HADOOP_HOME}\\bin;{system32};{os.environ['PATH']}"

# Create temp directories
import pathlib
pathlib.Path("C:/tmp/hive").mkdir(parents=True, exist_ok=True)
pathlib.Path("C:/tmp/spark-warehouse").mkdir(parents=True, exist_ok=True)

print("\n=== Starting Spark 3.4 ===")


spark = (
    SparkSession.builder
    .appName("bronze-to-silver-batch")
    .master("local[1]")  # Use single thread to avoid multi-threading issues
    
    # ---- PACKAGES for Spark 3.4 ----
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ])
    )
    
    # ---- S3A ----
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("STORAGE_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("STORAGE_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("STORAGE_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Windows-specific configs
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
    
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")  # Reduce noise

print("✅ Spark created OK")

try:
    print("\nAttempting to read from S3...")
    df = spark.read.json("s3a://camdoesdata/bronze/transactions_raw/")
    
<<<<<<< HEAD
print("Spark created OK")


# Prove S3A filesystem class is on the classpath
print("fs.s3a.impl =", spark.sparkContext._jsc.hadoopConfiguration().get("fs.s3a.impl"))

# Force a real read/action from S3
df = spark.read.json("s3a://camdoesdata/bronze/transactions_raw/")
print("About to show() ...")
df.limit(5).show(truncate=False)

print("Done.")


spark.stop()

=======
    count = df.count()
    print(f"✅ Read {count} records from S3")
    
    print("\nShowing first 5 rows:")
    df.limit(5).show(truncate=False)
    
    print("\nSchema:")
    df.printSchema()
    
    print("\n✅ Success!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
finally:
    spark.stop()
>>>>>>> dfc8fa1 (UPDATED CODE TO WORK ANYWHERE (pip hardcoded))
