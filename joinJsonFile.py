# please refer notes for broadcast join, shuffle join,  how to optimize joins
from pyspark.sql import *
from pyspark.sql.functions import bucket
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark=SparkSession.builder \
    .master("local[3]") \
    .enableHiveSupport() \
    .appName("WindowDemo") \
    .getOrCreate()

#default mode = permissive, i.e include corrupt_record column
json_df=spark.read.format("json").option("mode","dropMalFormed").option("header","true").load("data/JsonJoin1.json")
json_df2=spark.read.format("json").option("mode","permissive").option("header","true").load("data/JsonJoin2.json")

spark.sql("Create Database if not exists BucketPartition")
spark.sql("USE BucketPartition")

# or setCurrentDatabase
json_df.coalesce(1).write \
    .mode("overwrite") \
    .bucketBy(2,"dept_id") \
    .option("header","true") \
    .saveAsTable("BucketPartition.json_file_tab1")
# !!! WARNING --- always save table with db.tablename

json_df2.coalesce(1).write \
    .mode("overwrite") \
    .bucketBy(2,"dept_id") \
    .option("header","true") \
    .saveAsTable("BucketPartition.json_file_tab2")

# print(spark.catalog.listDatabases())
# print(spark.catalog.listTables("BucketPartition"))
#
# df1=spark.read.table("BucketPartition.json_file_tab1")
# df2=spark.read.table("BucketPartition.json_file_tab2")
#
# join_expr = df1.dept_id == df2.dept_id
# joined_df = df1.join(df2, join_expr, "inner").show()

# df1=(spark.read.format("json").option("header","true")\
#      .load("data/JsonJoin1.json"))
#
#
# bucket_json1 = json_df.join(json_df2,"dept_id","inner").show()

