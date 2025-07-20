from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark=SparkSession.builder \
    .master("local[3]") \
    .appName("AggregationsDemo") \
    .getOrCreate()

data_list=[
    ("Yash",28,8411,2,"Out"),
    ("Kish",23,5561,4,"NotOut"),
    ("vaishu",20,9552,2,"Out"),
    ("vaishu",21,9942,1,"NotOut"),
    ("Sai",18,20333,2,"Out"),
    ("yole", 68, 8461,1, "Out"),
    ("Dany", 83, 5566,1, "NotOut"),
    ("John", 10, 9442,2, "Out"),
    ("Noah", 22, 8892,4,"NotOut"),
    ("Lily", 14, 603,6,"Out")
]
rawDF = spark.createDataFrame(data_list).toDF("name","age","Score","numOfGame","status")
rawDF.printSchema()
rawDF.show()

# Aggregate Functions - DataFrame API - column string
grpDf = rawDF.select(count("*").alias("count *"),
                     sum("age").alias("sum age"),
                     max("numOfGame").alias("maxGamePlayed"),
                     avg("Score").alias("avg Score")
                     ).show()

# Aggregate Functions - SPARK SQL API
rawDF.createOrReplaceTempView("sparkSqlDF")
rawSQL = spark.sql("""select 
                            status,
                            count(*) as count,
                            sum((numOfGame*Score)) as sumScore
                            from sparkSqlDF
                            group by status""").show()

# Using Dataframe API selectExpr
SelectExpressionDF = rawDF.selectExpr("count(1) as count",
                                      "max(numOfGame) as maxGame",
                                      "avg(age) as avgAgePlayer").show()

#using DataFrame
rawDFF = (rawDF.withColumn("status",col("Score")) \
               .where(rawDF.age > 20) \
               .groupBy("status") \
               .agg(sum(expr("numOfGame * Score")).alias("ScoreSum")).show())


