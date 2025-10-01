import os
import sys
from pyspark import storagelevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from pyspark.sql.types import *
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark=SparkSession.builder.appName("String Function in pyspark").getOrCreate()

data = [("  alice  ",), ("BOB",), ("charlie brown",), ("daisy",)]
df = spark.createDataFrame(data, ["Name"])
df.show(truncate=True)
#Upper()
df.select(upper(col("Name")).alias("Uppercase")).show()
#trim()
df.select(trim(col("Name")).alias("After Trim")).show()
#ltrim()
df.select(ltrim(col("Name")).alias("After lTrim")).show()
#rtrim()
df.select(rtrim(col("Name")).alias("Ater rtrim")).show()
#Sub_String index
df.select(substring_index(col("Name"), " ", 1).alias("FirstWord")).show()
#substring()
df.select(substring(col("Name"),1,3).alias("Substring")).show()
#split()
df.select(split(col("Name")," ").alias("Split")).show()
#Repeat
df.select(repeat(trim(col("Name")), 2).alias("Repeated")).show(truncate=False)
#rpad()
df.select(rpad(trim(col("Name")), 10, "*").alias("RPad")).show()
#lpad
df.select(lpad("Name", 10, "!").alias("LPad")).show()
#regex_replace()
df.select(regexp_replace("Name","a","@").alias("regex_replace")).show()
#lower()
df.select(lower("name").alias("Lowercase")).show()
#regex_Exract()
df.select(regexp_extract(col("Name"), "([a-z]+)", 1).alias("RegexExtract")).show()
#length()
df.select(length("Name").alias("Length of string")).show()
#instr()
df.select(instr("Name","a").alias("Postion of string")).show()
#initcap()
df.select(initcap("Name").alias("Print the first letter as capitial")).show()