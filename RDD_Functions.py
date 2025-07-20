import pyspark
from pyspark.sql import *

spark=SparkSession.builder \
     .master("local[2]") \
     .appName("RDD Map/Filter functions") \
     .getOrCreate()
scc=spark.sparkContext.getOrCreate()

xMap=spark.sparkContext.parallelize([1,2,3,4,5,6,7,8,9])
x_map=xMap.map(lambda x:(x,x**2))
print(x_map.collect())

x_map_even=xMap.filter(lambda x:(x%2==0))
print(x_map_even.collect())

#FlatMap will take more inputs
y_map=xMap.flatMap(lambda x:(x,x**2,x*1000))
print(y_map.collect())
# print(xMap.glom().collect())

# mapPartitions - do operations for all partitions
yMap=scc.parallelize([1,2,3,4,5,6,7,8,9],2)
def summ(iterator):
     yield sum(iterator)
z=yMap.mapPartitions(summ)
print(z.collect())
print(z.glom().collect())

#map partitions along with index (partitionIndex,value)
def summ(indexVal,iterator):
     yield (indexVal,sum(iterator))
zi=yMap.mapPartitionsWithIndex(summ)
print(zi.collect())
print(zi.glom().collect())

#sample(withReplacement, fraction, seed=None)
# True = repeat, false = wont repeat, fraction =1 means 100% repaeat
samptrue = xMap.sample(True,1)
sampFal = xMap.sample(False,0.3
                      )
print(samptrue.collect())
print(sampFal.collect())

#union join or merge dataset
rdd1=scc.parallelize([11,23,4,56])
rdd2=scc.parallelize([41,53,55,46])

rdd1.union(rdd2).collect()
rdd2.intersection(rdd1).collect()
print(rdd1.distinct().collect())

# Group By Key - group the values by key
rddKey=scc.parallelize([("B",11),("A",23),("B",4),("A", 56)])
xx=rddKey.groupByKey()
print(rddKey.groupByKey().collect())
print([(j[0],[i for i in j[1]]) for j in xx.collect()])
# Same like
for jj in xx.collect():
     print(jj[0])
     for i in jj[1]:
          print(i)


# cache - store the transformation in memory
# persist - same like cache
# unpersist - delete the data
rdd1.persist(pyspark.StorageLevel.MEMORY_AND_DISK_DESER)  #Deserialize
rdd1.unpersist()
rdd1.persist(pyspark.StorageLevel.DISK_ONLY_2)
rdd1.unpersist()
# After every persist(), we have to unpersist to try different persist options
rdd1.persist(pyspark.StorageLevel.DISK_ONLY)
rdd1.cache()
rdd1.unpersist()
rdd1.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
rdd1.unpersist()


# Broadcast variable store variables in cache in memory
broadCastVar = scc.broadcast([1,2,3,4,5,6,7,8,9])
print(type(broadCastVar))
broadCastVar.value
help(broadCastVar)
broadCastVar.destroy()
broadCastVar.dump()
broadCastVar.load()
# to remove the variable in cache
broadCastVar.unpersist()

# Accumulators - added through an associative and commutative operation, like add()
accum_v=scc.accumulator(0)
scc.parallelize([1,2,3,4,5,6,7,8,9]).foreach(lambda x : accum_v.add(x))
accum_v.value

accum_v=scc.accumulator(5) # 5+ list
scc.parallelize([1,2,3,4,5,6,7,8,9]).foreach(lambda x : accum_v.add(x))
accum_v.value