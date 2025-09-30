import os
import sys

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark=SparkSession.builder.appName("DataFrame opearation").getOrCreate()

df=spark.read.csv("D:\Sales.csv",header=True,inferSchema=True)
df.filter(df.Sales>9000).show()

df.withColumn("new cost",col("cost")+1000).show()

df.groupBy("Profit").avg("Sales").show()

print(df.collect())

print(df.take(4))