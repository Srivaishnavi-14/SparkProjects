#Data is stored in JDBC such as Oracle,MySQL,PostgresSQL, NoSQL-MongoDB,Cassandra
#Colud -- Amazon redshift, Some Streaming platforms
#We can extract data from there in two ways - 1. through tools like informatica,amazon glue for batch processing
#2. we can directly get data through spark API, but only small amount can be taken at a time
#Spark API data extracting used for stream processing

#Q1. How to use DataFrameReader fr CSV,JSON, Parquet file

from pyspark import SparkConf,SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from lib.utils import *
from lib.logger import Log4j

spark=SparkSession.builder \
    .master("local[3]") \
    .appName("SparkSchemaDemo") \
    .getOrCreate()

logger=Log4j(spark)

personality_df = spark.read \
    .format("csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .load("data/personality_dataset.csv")

# spark.read \
#There are 3 types in Read mode, 1.FAILFAST 2.DROPMALFORMED  3. PERMISSIVE
#-- FAILFAST -- Immediately fail if find Malformed or corrupt data
#-- DROPMALFORMED -- Ignore Malformed or corrupt data
#-- PERMISSIVE -- Tries to parse all lines; puts malformed data in a special column called _corrupt_record.
#     .option("mode","FAILFAST") \
#     .option("dateFormat","M/d/y")


#if we want to use all files in a directory we can give wildcard *
personality_df.show(5)
print("CSV Schema"+ personality_df.schema.simpleString())

titanic_json = spark.read \
    .format("json") \
    .option("header","true") \
    .option("mode","FAILFAST") \
    .option("dateFormat",'M/d/y') \
    .load("data/titanic.json")
    # .option("inferSchema","true") \

titanic_json.show(5)
print("JSON Schema"+ titanic_json.schema.simpleString())

titanic_parquet = spark.read \
    .format("parquet") \
    .option("header", "true") \
    .load("data/titanic.parquet")
# .option("inferSchema","true") \

titanic_parquet.show(5)
print("Parquet Schema" + titanic_parquet.schema.simpleString())