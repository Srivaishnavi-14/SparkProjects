from pyspark import SparkConf
from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("Hello Spark") \
            .master("local[3]") \
            .getOrCreate()

#Another Way
    conf = SparkConf()
    conf.set("spark.app.name","Hello Spark")
    conf.set("spark.master","local[1]")
    spark2= SparkSession.builder \
            .config(conf=conf) .getOrCreate()
    spark.stop()
