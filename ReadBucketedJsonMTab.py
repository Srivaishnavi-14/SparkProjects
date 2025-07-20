# Note -- im reading managed table in seperate file rather than created file
# to read in seperate file we should enableHiveSupport() and mention "spark.sql.warehouse.directory"

from pyspark.sql.functions import bucket
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark=SparkSession.builder \
    .config("spark.sql.warehouse.dir","C:\CondaProjects\spark-warehouse") \
    .enableHiveSupport() \
    .master("local[2]") \
    .appName("Bucket Read Demo") \
    .getOrCreate()

print(spark.catalog.listDatabases())
print(spark.catalog.listTables("BucketPartition"))
# use table to read , bcoz we are reading from managed table
df1=spark.read.table("BucketPartition.json_file_tab1")
df2=spark.read.table("BucketPartition.json_file_tab2")

join_expr = df1.dept_id == df2.dept_id
joined_df = df1.join(df2, join_expr, "inner").show()
