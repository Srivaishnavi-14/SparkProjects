class Log4j:
    def __init__(self,spark):
        log4j = spark._jvm.org.apache.log4j
        conf=spark.sparkContext.getConf()
        app_name=conf.get("spark.app.name")  #hello Spark
        self.logger = log4j.LogManager.getLogger("learn.spark.practice.vaishu")

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)

