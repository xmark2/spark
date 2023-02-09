from pyspark import SparkConf
from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    # https: // spark.apache.org / docs / latest / configuration  # application-properties
    # conf = SparkConf()
    # conf.set("spark.app.name", "Hello Spark")
    # conf.set("spark.master", "local[3]")

    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    # spark = SparkSession.builder\
    #     .appName("Hello Spark")\
    #     .master("local[3]")\
    #     .getOrCreate()

    logger = Log4J(spark)

    logger.info("Starting HelloSpark")
    # Your processing code

    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())

    logger.info("Finished HelloSpark")
    spark.stop()

    # print("hello")
