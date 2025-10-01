#Creating dataframe
import os
import sys
from pyspark.sql.functions import col,lit
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark=SparkSession.builder.appName("PySpark Sql").getOrCreate()

df=spark.read.csv("D:\Sales.csv",header=True,inferSchema=True)
#WithCoulumn using
df.withColumn("Product",lit("Electonics")).show()

#Rename Column
df.withColumnRenamed("Profit","Revenue").show()

#Operation in WithColumn
df.withColumn("Newcost",col("cost")*2).show()
#Aggregation and grouping
df.count()
#
# Group by Region and calculate average Sales
df.groupBy("Region").avg("Sales").show()
#Multiple Aggregation
df.groupBy("Region").agg({"Product":"max","Sales":"Count"}).show()

#4. Sorting and Ordering
df.orderBy("Product").show()
# Sort descending
df.orderBy(df.Product.desc()).show()


#5. SQL Queries
# Register DataFrame as SQL table
df.createOrReplaceTempView("Sales_Detail")
spark.sql("select * from Sales_Detail").show()
# WHERE clause
spark.sql("select *from Sales_Detail where Sales >10000").show()

# GROUP BY + Aggregation
spark.sql("select Region,avg(Cost) as avg_Cost from Sales_Detail Group by Region").show()
# ORDER BY
spark.sql("select Region,Cost from Sales_Detail order by Cost desc").show()