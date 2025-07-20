#DataFrame Writer is used for transforming data frames and read to write
# the files are partitioned into many dataframes before storing them, so we can customize the
# file records by partioning the data manually using repartition(), partitionBy(), bucketBy() etcc..
# #General Syntax
#       DataFrameWrite.format()
#                     .option()
#                     .partitionBy(col1,col2)----> like (Key,Value)
#                     .bucketBy(n,col1,col2)
#                     .sortBy() #always used with bucketBy
#                     .maxRecordsPerFile  --> used to limit the number of records per file
#                     .save()

#Q1. How to use DataFrameWriter fr CSV,JSON, Parquet file

from pyspark import SparkConf,SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from lib.utils import *
from lib.logger import Log4j

spark=SparkSession.builder \
    .master("local[3]") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.13:4.0.0") \
    .appName("SparkSchemaDemo") \
    .getOrCreate()

logger=Log4j(spark)

# spark.read \
#There are 4 types in Write mode, 1.append 2.overwirte  3. errorIfExists 4.Igonre
# -- append-- will append the lines in if existing filename found
# -- overwrite-- will clean the directory and write the file
# --- errorIfExists-- will throw an error if find existing file name
# -- Ignore -- will ignore , do nothing

#parquet file format is the default way of writing file
titanic_parquet = spark.read \
    .format("parquet") \
    .load("data/titanic.parquet")
    # .option("inferSchema","true")
titanic_parquet.write \
    .format("avro") \
    .mode("overwrite") \
    .save("dataSink/avro/")

print("Num Partitions before: " + str(titanic_parquet.rdd.getNumPartitions()))
#TO KNOW THE COUNT OF PARITITON ID
titanic_parquet.groupBy(spark_partition_id()).count().show()

#repartition manually using repartition
repart_df=titanic_parquet.repartition(3)
print("Num Partitions Afer: " + str(repart_df.rdd.getNumPartitions()))
repart_df.groupBy(spark_partition_id()).count().show()

repart_df.write \
    .format("avro") \
    .mode("overwrite") \
    .save("dataSink/avro/")

#repartition manually based on column using partitionBy(colname,colname)
#In this example, the column sex and survived will be removed from data file and transformed into seperate folder
#we can set maxRecordPerFile as well
titanic_parquet.write \
    .format("avro") \
    .mode("overwrite") \
    .partitionBy("Survived","Sex") \
    .option("maxRecordsPerFile",65) \
    .save("dataSink/avro/")