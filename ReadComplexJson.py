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

# Example Read Single line json file
df_S = spark.read.option("singleLine","True") \
            .json("/path/of/json").show()

# Read Multiline json file
df_M = spark.read.option("multiLine","True") \
            .json("/path/of/json").show()

# Read Complex Json - Nested Single Line Json
# Declare schema
AddressSchema = StructType([
    StructField("State", StringType(), False),
    StructField("City", StringType(), False)
])
# pass StructType as DataType
custSchma=StructType([
    StructField("name", StringType(), False),
    StructField("Age", StringType(), False),
    StructField("Address", AddressSchema)
])
df_NS = spark.read.option("singleLine","True").schema(custSchma).json("/path/of/json").show()

# Nested Multi Line Json
df_NM\
    = spark.read.option("multiLine","True").schema(custSchma).json("/path/of/json").show()

#  Types
# 1. Struct - dict
# 2. Array - list
# 3. map

# To flatten complex datatype data
# We can use explode() function for array datatype