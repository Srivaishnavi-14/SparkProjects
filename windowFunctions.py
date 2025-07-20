from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark=SparkSession.builder \
    .master("local[3]") \
    .appName("WindowDemo") \
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

#we can give negative range for taking preceding values
precedng_range = Window.partitionBy("status") \
                    .orderBy("age") \
                    .rangeBetween(-2,Window.currentRow)
follow_range = Window.partitionBy("status") \
                    .orderBy("age") \
                    .rangeBetween(1,Window.unboundedFollowing)
num_range = Window.partitionBy("status") \
                    .orderBy("age") \
                    .rangeBetween(1,2)

sum_window = Window.partitionBy("status") \
                    .orderBy("age") \
                    .rangeBetween(Window.currentRow, 2)

# create new colum for calculated window function
sumUp= rawDF.withColumn("SumUp",sum("numOfGame").over(sum_window)).show()