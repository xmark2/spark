import sys
from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

if __name__ == "__main__":

    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    logger.info("Starting HelloSpark")
    # Your processing code
    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())

    # https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html?highlight=csv
    survey_df = load_survey_df(spark, sys.argv[1])
    # survey_df.show()

    partitioned_survey_df = survey_df.repartition(2)

    count_df = count_by_country(partitioned_survey_df)

    logger.info(count_df.collect())

    ## while the code running you can open Spark UI
    ## http://localhost:4040/jobs/

    input("Press Enter")
    logger.info("Finished HelloSpark")

    spark.stop()
