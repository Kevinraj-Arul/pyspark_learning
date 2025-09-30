import os
import sys

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("ActionsExample").getOrCreate()

# Create DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

#collect
co=df.collect()
print(co)

#Count
row=df.count()
print("Number of Rows:",row)

#First
f=df.first()
print("First column:",f)

