import os
import sys

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder\
        .appName("Simple Dataframe")\
        .master("local[*]")\
        .getOrCreate()


df=spark.read.csv("D:\Sales.csv",header=True,inferSchema=True)

df.show()
