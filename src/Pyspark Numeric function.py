import os
import sys
from pyspark import storagelevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, min, max, round, abs
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from pyspark.sql.types import *
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("Numeric Function").getOrCreate()

data = [
    (1, 10.5),
    (2, -20.3),
    (3, 30.7),
    (4, -40.9),
    (5, 50.2)
]
columns=["id","value"]
df=spark.createDataFrame(data,columns)
df.show()

#SUM()
df.select(sum("value").alias("Sum of value")).show()
#AVG()
df.select(avg("value").alias("avg of value")).show()
#max
df.select(max("value").alias("max of value")).show()
#Min
df.select(min("value").alias("Min of value")).show()
#ROUND()
df.select("value",round("value",0).alias("Rounded")).show()
#ABS
df.select("value",abs("value").alias("ABS value")).show()

