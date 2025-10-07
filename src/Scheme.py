import os
import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, DateType
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("Scheme").getOrCreate()
schema=StructType([
    StructField("Patientid",IntegerType(),True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Score", DoubleType(), True),
    StructField("VisitDate", StringType(), True)

])
data = [
    (101, "John", 25, 85.5, "2025-09-30"),
    (102, "Alice", 30, 92.0, "2025-10-01"),
]
df=spark.createDataFrame(data, schema)
df.printSchema()
df.show()