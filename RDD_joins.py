from pyspark.sql import *
from pyspark.sql.functions import bucket
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark=SparkSession.builder \
    .master("local[3]") \
    .appName("WindowDemo") \
    .getOrCreate()
scc=spark.sparkContext.getOrCreate()

# Joins in RDD
rddJoin1=scc.parallelize([("B",11),("A",23),("B",4),("A", 56)])
rddJoin2=scc.parallelize([("C",12),("D",33),("B",5),("A", 66)])
rddJoined = rddJoin1.join(rddJoin2)
print(rddJoined.collect())

rightOutJoin = rddJoin1.rightOuterJoin(rddJoin2)
print(rightOutJoin.collect())

LEFTOutJoin = rddJoin1.leftOuterJoin(rddJoin2)
print(LEFTOutJoin.collect())

# fullOuterJoin, cartesian() - crossjoin