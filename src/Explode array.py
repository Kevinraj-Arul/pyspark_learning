import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, posexplode_outer, col, create_map, lit
from setuptools.command.alias import alias

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.appName("ExplodeFunctions").getOrCreate()

data = [
    (1, ["fever", "cough", "headache"]),
    (2, ["cold", "fatigue"]),
    (3, None)
]
columns = ["PatientID", "Symptoms"]
df = spark.createDataFrame(data, columns)
"""df.show(truncate=False)
df_explode=df.select(col("PatientID"),explode(col("Symptoms").alias("Explode value")))
df_explode.show()
df_explode_outer=df.select(col("PatientID"),explode_outer(col("Symptoms")).alias("Explode value"))
df_explode_outer.show()"""
df_posexplode_outer=df.select(col("PatientID"),posexplode_outer(col("Symptoms")).alias("Explode value","Position"))
df_posexplode_outer.show()
