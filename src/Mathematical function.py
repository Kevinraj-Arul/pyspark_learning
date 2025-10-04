import os
import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("Mathematical function").getOrCreate()
data = [
    (1, -4.5),
    (2, 3.2),
    (3, 0.0),
    (4, 2.5)
]
columns = ["ID", "Value"]
df=spark.createDataFrame(data,columns)
df.show()
df.select("Value",(abs("Value")).alias("New Value")).show()
df.select("Value",(floor("Value")).alias("New Value")).show()
df.select("Value",(ceil("Value")).alias("New Value")).show()
df.select("Value",(exp("Value")).alias("New Value")).show()
df.select("Value",(log("Value")).alias("New Value")).show()
df.select("Value",(sqrt("Value")).alias("New Value")).show()