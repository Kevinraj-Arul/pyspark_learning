import os
import sys
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import conv

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("Conversion function").getOrCreate()
data = [
    ("101", "85.5", "2025-09-30", "2025-09-30 14:35:00"),
    ("102", "92.0", "2025-10-01", "2025-10-01 09:00:00")
]
columns = ["PatientID", "Score", "VisitDate", "VisitTimestamp"]
df=spark.createDataFrame(data,columns)
df.show()
df_casted = df.select(
    col("PatientID").cast("int").alias("PatientID_int"),
    col("Score").cast("double").alias("Score_double"),
    col("VisitDate").cast("date").alias("VisitDate_converted"),
    col("VisitTimestamp").cast("timestamp").alias("VisitTimestamp_converted")
)
df_casted.printSchema()
df_casted.show()
df.withColumn("PatientID_decimal", conv(col("PatientID"), 2, 10)).show()