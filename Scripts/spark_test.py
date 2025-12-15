import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


os.environ.setdefault("HADOOP_HOME", "C:\\hadoop")
os.environ.setdefault("hadoop.home.dir", "C:\\hadoop")
os.environ["PATH"] += ";C:\\hadoop\\bin"


builder = SparkSession.builder.appName("spark-test")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

