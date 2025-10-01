from pyspark.sql import SparkSession
from pyspark.sql .types import *
from pyspark.sql.types import StructType



spark=SparkSession.builder.appName("General DataFrame Function").getOrCreate()
df=spark.read.csv("D:\Sales.csv",header=True,inferSchema=True)
#show fucnction
df.show()
#collect
print(df.collect())
#Taken
print(df.take(6))
#printSchema()
df.printSchema()
#count()
print("Number of rows:",df.count())
#select()
df.select("Product","Cost").show()
#filter() / where()
df.filter(df.Sales>10000).show()
df.where(df.Cost == 11000).show()
#like() → SQL pattern matching
df.filter(df.Region.like("n%")).show()
#sort()
df.sort("Sales").show()
df.sort(df.Sales.desc()).show()
#describe()
df.describe(["Sales"]).show()
#columns
print(df.columns)
