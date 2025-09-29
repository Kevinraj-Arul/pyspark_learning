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

datae=[(1,"kevin"),(2,"raj")]

names=StructType([StructField (name='id',dataType=IntegerType()),StructField(name='name',dataType=StringType())])

df=spark.createDataFrame(datae,names)


df.show()

df.printSchema()