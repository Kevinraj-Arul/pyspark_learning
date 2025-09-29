
from pyspark.sql import SparkSession


spark = SparkSession.builder\
        .appName("Spark Intro")\
        .master("local[*]")\
        .getOrCreate()