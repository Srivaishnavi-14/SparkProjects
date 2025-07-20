from pyspark import SparkConf,SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from lib.utils import *
from lib.logger import Log4j
# Hive is the Spark Database storage, so we should enable Hive

spark=SparkSession.builder \
    .master("local[3]") \
    .enableHiveSupport() \
    .appName("SparkSchemaDemo") \
    .getOrCreate()

logger=Log4j(spark)

titanic_parquet = spark.read \
    .format("parquet") \
    .load("data/titanic.parquet")

#Default Database is "Defaulut"
# if we want specific Database
spark.sql("CREATE DATABASE IF NOT EXISTS titanic")
spark.catalog.setCurrentDatabase("titanic")

#bucket by used to restrict partitions based on partitionBy()- i.e col1,col2

titanic_parquet.write \
    .mode("overwrite") \
    .bucketBy(2,"Sex","Survived") \
    .sortBy("Sex","Survived") \
    .saveAsTable("titanic.titanic_ship")

print("DB LIST: " + str(spark.catalog.listDatabases()))
print("Table LIST: " + str(spark.catalog.listTables("titanic")))


# titanic_parquet.write \
#     .format("avro") \
#     .mode("overwrite") \
#     .save("dataSink/avro/")




