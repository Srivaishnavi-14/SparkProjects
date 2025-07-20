# CASE WHEN THEN
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark=SparkSession.builder \
    .master("local[3]") \
    .appName("MiscDemo") \
    .getOrCreate()

# here year is not proper
data_list=[
    ("Ravi",28,81),
    ("Kavi",23,6),
    ("vaishu",20,2002),
    ("vaishu",21,2002),
    ("Sai",18,2003)
]
rawDF = spark.createDataFrame(data_list).toDF("name","age","year")
rawDF.printSchema()
rawDF.show()

# we can set AutoIncrement id using monotonically_increasin_id() method and withColumn() method
# if column didnt present , it will create new column
rawDF1=rawDF.withColumn("id", monotonically_increasing_id())
rawDF1.show()

#CASE WHEN - we can change the improper date to proper date using case in sql
rawDF2 = rawDF1.withColumn("year",expr("""
case when year < 21 then year + 2000
when year < 100 then year + 1900
else year
end"""))
rawDF2.printSchema()
rawDF2.show()

# if we want to case year
# Method 1
rawDF3 = rawDF1.withColumn("year",expr("""
case when year < 21 then cast(year as int) + 2000
when year < 100 then cast(year as int) + 1900
else year
end"""))
rawDF3.show()
rawDF3.printSchema()

#Method 2
rawDF4 = rawDF1.withColumn("year",col("year").cast(IntegerType())) \
                .withColumn("id",round(col("id"),2)) \
                .withColumn("age",col("age").cast(IntegerType()))
rawDF4.show()

# Case usingg DataFrame API SQL , Column Strings
DFraw = rawDF1.withColumn("year", \
                        when(col("year") < 21, col("year") + 2000) \
                        .when(col("year") < 100, col("year") + 1900) \
                        .otherwise(col("year"))
                        )
DFraw.show()

#we can drop columns, dropDuplicates based on name and year column, sort("year")--> ascending
# or sort(expr("year desc")) --> expr() due to sql expression desc
droppingCol = rawDF2.drop("age") \
                    .dropDuplicates(["name","year"]) \
                    .sort(expr("year desc"))
droppingCol.show()


