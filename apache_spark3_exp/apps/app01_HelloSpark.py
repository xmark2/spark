import sys
from pyspark.sql import SparkSession
from apache_spark3_exp.apps.lib import Log4j, get_spark_app_config, load_survey_df, count_by_country


class HelloApp:

    def __init__(self, path_conf, app_id):

        conf = get_spark_app_config(path_conf, app_id)

        self.spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

        self.logger = Log4j(self.spark)

    def check_arg_data(self):
        if len(sys.argv) != 2:
            self.logger.error("Usage: HelloSpark <filename>")
            sys.exit(-1)

    def show_count_by_country(self):

        self.check_arg_data()

        self.logger.info("Starting HelloSpark")

        survey_raw_df = load_survey_df(self.spark, sys.argv[1])
        partitioned_survey_df = survey_raw_df.repartition(2)
        count_df = count_by_country(partitioned_survey_df)
        count_df.show()

        self.logger.info(count_df.collect())

        ## while the code running you can open Spark UI
        ## http://localhost:4040/jobs/

        input("Press Enter")

        self.logger.info("Finished HelloSpark")
        self.spark.stop()
