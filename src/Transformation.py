#Map
import os
import sys

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, DataType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder\
        .appName("Simple Dataframe")\
        .master("local[*]")\
        .getOrCreate()

rdd=spark.sparkContext.parallelize([1,2,3,4,5])

mapp=rdd.map(lambda x:x*2)

print(mapp.collect())

#filter
datas=[("Alice",25),("Bob",30),("Charile",35)]
schemas = StructType([
    StructField("name", StringType(), True),
    StructField("Age", IntegerType(), True)
])

df=spark.createDataFrame(datas,schemas)

from pyspark.sql.functions import col
filtered=df.filter(df.Age>29)

filtered.show()

#Select
datas=[("Alice",25),("Bob",30),("Charile",35)]
schemas = StructType([
    StructField("name", StringType(), True),
    StructField("Age", IntegerType(), True)
])
df=spark.createDataFrame(datas,schemas)
seleted=df.select("name")
seleted.show()

#Withcoloumn
from pyspark.sql.functions import col
datas=[("Alice",25),("Bob",30),("Charile",35)]
schemas = StructType([
    StructField("name", StringType(), True),
    StructField("Age", IntegerType(), True)
])
new=df.withColumn("new_Column_age",col("Age")+5)
new.show()

#Group BY
datas = [("Alice", "Math", 85),
        ("Alice", "English", 78),
        ("Bob", "Math", 92),
        ("Bob", "English", 88)]
schemas=StructType([
    StructField(name="Name",dataType=StringType(),nullable = True),
    StructField(name="Subject",dataType=StringType(),nullable = True),
    StructField(name="Score",dataType=IntegerType(),nullable = True)
])

df = spark.createDataFrame(datas,schemas)

new=df.groupBy("Name").avg("Score")

new.show()

#Joins
students = [("1", "Alice"), ("2", "Bob"), ("3", "Charlie")]
marks = [("1", 85), ("2", 90), ("4", 70)]

new_students=spark.createDataFrame(students,["Id","Name"])
new_marks=spark.createDataFrame(marks,["Id","Name"])

new_join=new_students.join(new_marks,new_students.Id == new_marks.Id, "inner")

new_join.show()



