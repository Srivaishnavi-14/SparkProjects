# By default, Spark will infer the schema of the JSON file.
# Easy and fast for ad-hoc analysis, Convenient if the structure is simple and consistent
# Slower for large files , Nested Json
#In that Case, we can manually define schema using StructType,StructField and data types for each columns
#USER DEFINED FUNCTION - UDF , if we create function and using it in spark function we should
# first reister it using udf(functionName,ReturnTypeOfFunction) --> udf(parseDate.StringType())
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

#UDF
def example():
    return "None"

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("email", StringType(), True)
])
spark=SparkSession.builder \
    .master("local[3]") \
    .appName("SparkSchemaDemo") \
    .getOrCreate()

df = spark.read.schema(schema).json("data.json")

#Register udf in DataFrame SQL
new_Example=udf(example,StringType())
#With Column used to change values in a column, we can use UDF function in expression
new_df=df.withColumn("name",new_Example())
#if we want to check whether this function present in CATLOG
[print(f) for f in spark.catalog.listFunctions() if "example" in f.name]

#if we want to register using SPARK SQL---> it will create on entry in catalog
spark.udf.register("Name_of_udf",example,StringType())
[print(f) for f in spark.catalog.listFunctions() if "example" in f.name]


# we can manually create dataframe
data_list=[
    ("Ravi",28,)
    ("Kavi",23)
]
rawDF = spark.createDataFrame(data_list).toDF("name","age")
rawDF.printSchema()

