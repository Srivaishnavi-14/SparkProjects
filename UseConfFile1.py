import sys

from pyspark import SparkConf,SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from lib.utils import *
from lib.logger import Log4j

conf=get_spark_session()

spark=SparkSession.builder \
    .config(conf=conf).getOrCreate()

logger=Log4j(spark)

conf_out=spark.sparkContext.getConf()

logger.info("Starting Spark Session")
logger.info(conf_out.toDebugString())

#Sample program
#Option method takes key value pair
#DataFramereader is going to use the header row and infer the column names
#inferSchema - datatypes for the column --- This infer schema allow dataframe to read a portion of file and guess a datatype of column
survey_df = load_survey_df(spark, sys.argv[1])

# partioned_df = survey_df.repartition(2) --> forcefully dividing the dataframe in to Two

#partition dataframe using SQLDataFrame functions
part1_df = survey_df.select("Personality",)
part2_df = part1_df.groupBy("Personality")
part3_df = part2_df.agg(count("Personality").alias("Personality_count"))
part3_df.show()
# survey_df.show()

#Alternate Simple Way #Transformations-groupBy,agg,select #Actions - show()
survey_df.select("Personality","Time_spent_Alone").groupBy("Personality").agg(sum("Time_spent_Alone")) \
        .show()
inp=input("user :")

logger.info("Stopping Spark Session")
# spark.stop()