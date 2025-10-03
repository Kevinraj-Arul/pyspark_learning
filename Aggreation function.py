import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark=SparkSession.builder.appName("Aggregate function").getOrCreate()
data = [
    ("John", "Cardiology", 5000),
    ("Alice", "Cardiology", 6000),
    ("Bob", "Neurology", 5500),
    ("Emma", "Neurology", 7000),
    ("Tom", "Orthopedics", 4000),
    ("Mike", "Orthopedics", 4000)
]
columns = ["Name", "Department", "Salary"]

df = spark.createDataFrame(data, columns)
df.show()
#mean
df.select(mean("salary").alias("Mean_salary")).show()
#avg
df.select(avg("salary")).alias("Avg_salary").show()
#collect_list()
df.groupBy("Department").agg(collect_list("name")).alias("collected list").show()
#collect_set()
df.groupBy("Department").agg(collect_set("name")).alias("collected_set").show()
#countDistinct
df.select(countDistinct("name")).alias("No.of Distinct").show()
#count()
df.groupBy("Department").agg(count("Name")).alias(("Nof of count")).show()
#first()
df.groupBy("Department").agg(first("Name")).alias("First name").show()
#last()
df.groupby("Department").agg(last("Name")).alias("Last name").show()
#max()
df.groupBy("Department").agg(max("salary")).alias("Max salary").show()
#min()
df.groupBy("Department").agg(min("salary")).alias("Min salary").show()
#sum()
df.groupBy("Department").agg(sum("salary")).alias("sum salary").show()
