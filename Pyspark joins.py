import os
import sys
from pyspark.sql import SparkSession
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.appName("JoinExamples").getOrCreate()

# Patients Data
patients_data = [
    (1, "John", 101),
    (2, "Alice", 102),
    (3, "Bob", 103),
    (4, "Emma", 104),
    (5, "Tom", None)   # Tom not assigned to any dept
]
patients_cols = ["PatientID", "Name", "DeptID"]

patients_df = spark.createDataFrame(patients_data, patients_cols)

# Departments Data
departments_data = [
    (101, "Cardiology"),
    (102, "Neurology"),
    (103, "Orthopedics"),
    (106, "Oncology")  # Department with no patients
]
departments_cols = ["DeptID", "Department"]

departments_df = spark.createDataFrame(departments_data, departments_cols)

print("Patients DataFrame:")
patients_df.show()

print("Departments DataFrame:")
departments_df.show()
#INNER JOIN
inner_df=patients_df.join(departments_df,patients_df.DeptID == departments_df.DeptID,"inner").show()
#CROSS JOIN 
cross_df=patients_df.crossJoin(departments_df).show()
#OUTER JOIN (FULL OUTER JOIN)
full_join=patients_df.join(departments_df,patients_df.DeptID == departments_df.DeptID,"outer").show()
#LEFT JOIN
left_df=patients_df.join(departments_df,patients_df.DeptID == departments_df.DeptID,"left").show()
#LEFT SEMI JOIN
left_df=patients_df.join(departments_df,patients_df.DeptID == departments_df.DeptID,"left_semi").show()
#Left anti join
left_anti_df=patients_df.join(departments_df,patients_df.DeptID == departments_df.DeptID,"left_anti").show()
