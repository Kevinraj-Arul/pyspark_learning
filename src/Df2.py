import os
import sys
from pyspark.sql.types import *
from pyspark.sql import SparkSession
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder\
        .appName("Simple Dataframe")\
        .master("local[*]")\
        .getOrCreate()

datae=[(1,"kevin"),(2,"raj")]

names=["id","name"]

df=spark.createDataFrame(datae,names)

df.show()