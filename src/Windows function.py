import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col,row_number, rank, dense_rank,lag,lead, sum, avg, max, min,cume_dist
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("Windows Function").getOrCreate()

data = [
    ("Alice", "HR", 3000),
    ("Bob", "HR", 4000),
    ("Charlie", "IT", 5000),
    ("David", "IT", 6000),
    ("Eve", "Finance", 7000),
    ("Frank", "Finance", 4000)
]

columns = ["Name", "Department", "Salary"]
df=spark.createDataFrame(data,columns)
windowSpec = Window.partitionBy("Department").orderBy(col("Salary").desc())
df.show()

#Row Number
df.withColumn("Row_number",row_number().over(windowSpec)).show()
#rank
df.withColumn("Rank",rank().over(windowSpec)).show()
#Denserank
df.withColumn("Dense_rank",dense_rank().over(windowSpec)).show()
#lag
df.withColumn("lag",lag("Salary",1).over(windowSpec)).show()
#lead
df.withColumn("Lead",lead("Salary",1).over(windowSpec)).show()
#avg as same as min,max,sum,count
df.withColumn("Average",avg("Salary").over(windowSpec)).show()
#cumulative_disk
df.withColumn("cume_dists",cume_dist().over(windowSpec)).show()
