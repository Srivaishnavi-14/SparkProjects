from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark=SparkSession.builder \
    .master("local[3]") \
    .appName("WindowDemo") \
    .getOrCreate()

employees_data = [
    (1, "Alice", 10),
    (2, "Bob", 20),
    (3, "Charlie", 30),
    (4, "David", 10),
    (5, "Eva", 40),
    (6, "Frank", None),
    (7, "Grace", 50)
]
employees_data = spark.createDataFrame(employees_data).toDF("emp_id", "name", "dept_id")
departments_data = [
    (101, 10, "HR"),
    (102, 20, "Engineering"),
    (103, 30, "Finance"),
    (104, 40, "Marketing"),
    (105, 60, "Legal"),
    (106, 70, "R&D"),
    (107, None, "Unknown")
]
departments_data = spark.createDataFrame(departments_data,["emp_id", "dept_id", "dept_name"])
# employees_data.show()
# departments_data.show()

#Joins
#Method 1 - Use Alias for select column(ambiguity) that is present in both table
employees_data = employees_data.alias("employees_alias")
departments_data = departments_data.alias("departments_alias")

join_expr = employees_data.dept_id == departments_data.dept_id
# Easy Way
# Replace null values with coalesce()
Easy_joined_dfv= employees_data.join(departments_data,"dept_id","outer")\
                                .withColumn("employees_alias.emp_id",expr("coalesce(employees_alias.emp_id,employees_alias.dept_id)"))\
                                .withColumnRenamed("employees_alias.emp_id","employeetable_Id")\
                                .withColumn("dept_name",expr("coalesce(dept_name,'Unknown')"))\
                                .withColumn("name",expr("coalesce(name,dept_name)"))
Easy_joined_dfv.show()

joined_dfv= (employees_data.join(departments_data,join_expr,"inner") \
             .select("employees_alias.dept_id","dept_name"))

joined_dfv.show()

#Method 2 - WithColumnRenamed()
employees_renameDId = employees_data.withColumnRenamed("dept_id","emp_dept_id")
joined_dfv_2= (employees_renameDId.join(departments_data,
              employees_renameDId.emp_dept_id==departments_data.dept_id,
                "inner") \
             .select("emp_dept_id","dept_name"))
joined_dfv_2.show()

#Method 3 - Drop one same column in any table

joined_dfv_3= (employees_data.join(departments_data, join_expr,"inner") \
              .drop(employees_data.dept_id) \
             .select("dept_id","dept_name"))
joined_dfv_3.show()

