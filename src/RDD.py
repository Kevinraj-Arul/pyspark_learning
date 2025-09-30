#Rdd Creation
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import *
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName('rdd').getOrCreate()

rdd = spark.sparkContext.parallelize([1,2,4])

print(rdd.collect())

#Reading  file

rdd=spark.sparkContext.textFile("D:\Sales.csv")
print(rdd.collect())

#Converting of rdd to dataframe
rdd = spark.sparkContext.parallelize([("Alice", 25), ("Bob", 30), ("Charlie", 35)])

df=rdd.toDF(["Name","Age"])
df.show()

#Using SparkSession.createDataFrame()

spark = SparkSession.builder.appName("RDDtoDF").getOrCreate()
rdd = spark.sparkContext.parallelize([("Alice", 25), ("Bob", 30), ("Charlie", 35)])
Schema=StructType([
    StructField(name="Name",dataType=StringType(),nullable=True),
    StructField(name="Age",dataType=IntegerType(),nullable=True)
])
df=spark.createDataFrame(rdd,Schema)
df.show()