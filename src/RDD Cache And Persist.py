#Rdd Creation
import os
import sys
from pyspark import storagelevel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import *
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark=SparkSession.builder.appName("Rdd cache and Persist").getOrCreate()

rdd=spark.sparkContext.parallelize([1,2,3,4,5])

square=rdd.map(lambda x:x*2)
square.cache()
print("SQUARE VALUES:",square.collect())
print("SQUARE VALUES:",square.count())

#Rdd Persist
rdd_persist=rdd.map(lambda x:x+10)
rdd_persist.persist(storagelevel.MEMORY_AND_DISk)
print("Persist:",rdd_persist)
