import os
import sys
from pyspark.sql import SparkSession
from pyspark .sql.functions import *
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("Array Function").getOrCreate()
data = [
    (1, ["fever", "cough", "headache"]),
    (2, ["cold", "cough"]),
    (3, ["fever", "fatigue"]),
    (4, [])
]
columns = ["PatientID", "Symptoms"]
df = spark.createDataFrame(data, columns)
df.show(truncate=False)
df.select(col("PatientID"),array(lit("BP"),lit("Sugar"), lit("Cholesterol")).alias("New Array")).show(truncate=False)
df.select(col("PatientID"),col("Symptoms"),array_contains(col("Symptoms"),"cough").alias("Contains")).show()
df.select(col("PatientID"),col("Symptoms"),size(col("Symptoms")).alias("Length of array")).show(truncate=False)
df.select(col("PatientID"),col("Symptoms"),array_position(col("Symptoms"),"cough").alias("Position of value")).show(truncate=False)
df.select(col("PatientID"),col("Symptoms"),array_remove(col("Symptoms"),"cough").alias("Remove value")).show(truncate=False)
df.select("PatientID",concat(col("Symptoms"),array(lit(6),lit(7))).alias("concat")).show(truncate=False
df.select("PatientId",array_distinct(col("Symptoms")).alias("Distinct values")).show(truncate=False)