import os
import sys
from pyspark.sql import SparkSession

from pyspark.sql.types import *
from pyspark.sql.functions import *
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark=SparkSession.builder.appName("Date and Time").getOrCreate()

data = [("2025-09-25",), ("2025-03-10",), ("2024-12-15",)]
columns = ["date_str"]

df=spark.createDataFrame(data,columns)
df.show()
#CURRENT_DATE()
df.withColumn("Today",current_date()).show()
#CURRENT_TIMESTAMP()
df.withColumn("Current date and time",current_timestamp()).show(truncate=False)
#DATE_ADD()
df.withColumn("New Date",date_add(to_date("date_str"),10)).show()
#DATEDIFF()
df.withColumn("Diifdate",date_diff(current_date(),to_date("date_str"))).show()
#YEAR()
df.withColumn("year",year(to_date("date_str"))).show()
#MONTH()
df.withColumn("Month",month(to_date("date_str"))).show()
#DAY() (DAYOFMONTH)
df.withColumn("Days of month",dayofmonth(to_date("date_str"))).show()
#TO_DATE()
df.withColumn("To_dated",to_date("date_str")).show()
#DATE_FORMAT()
df.withColumn("formatted",date_format(to_date(("date_str"),"dd-mmm-yyyy"))).show()