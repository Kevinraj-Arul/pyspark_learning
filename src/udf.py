import os
import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("UDF Function").getOrCreate()
data = [
    (1, "John", 25),
    (2, "Alice", 30),
    (3, "Bob", 22)
]
columns = ["ID", "Name", "Age"]
df=spark.createDataFrame(data,columns)
df.show()
#Creating python function
def age_category (age):
    if age <25:
        return ("Young")
    elif age <30:
        return ("Adult")
    else:
        return ("Senior")
#CReating register for function
age_udf=udf(age_category,StringType())
#udf to dfframe column
with_age=df.withColumn("Category",age_udf(col("age")))
with_age.show()
#Register udf to  Sql
spark.udf.register("age_category_sql",age_category,StringType())
df.createOrReplaceTempView("Ages")
spark.sql("""select ID,Name,Age,age_Category_sql(age)as category from Ages""").show()
