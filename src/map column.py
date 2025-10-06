import os
import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import create_map
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.appName("MapFunctions").getOrCreate()

data = [
    (1, {"BP": 120, "Sugar": 95, "Cholesterol": 180}),
    (2, {"BP": 130, "Sugar": 110}),
    (3, None)
]
columns = ["PatientID", "TestResults"]
df = spark.createDataFrame(data, columns)
df.show(truncate=False)
#keymap
df_keys=df.select(col("patientID"),map_keys(col("TestResults")).alias("Keys"))
df_keys.show(truncate=False)
df_values=df.select(col("patientID"),map_values(col("TestResults")).alias("Keys"))
df_keys.show(truncate=False)
df_explodes=df.select(col("patientID"),explode(col("TestResults")).alias("Keys","Values"))
df_keys.show(truncate=False)