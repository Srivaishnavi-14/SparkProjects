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


#partition dataframe using SQLDataFrame functions
survey_df.createOrReplaceTempView("survey_tbl")
part1_df = spark.sql("select * from survey_tbl")
part1_df.show()
# survey_df.show()

logger.info("Stopping Spark Session")
# spark.stop()