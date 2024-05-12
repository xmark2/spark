import sys
from pyspark.sql import SparkSession
from apache_spark3_exp.apps.lib import Log4j, get_spark_app_config, load_survey_df


class SparkSqlApp:

    def __init__(self, path_conf, app_id, path_data):

        conf = get_spark_app_config(path_conf, app_id)
        self.path_data = path_data

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

        survey_raw_df = load_survey_df(self.spark, self.path_data)

        survey_raw_df.createOrReplaceTempView("survey_tbl")
        count_df = self.spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")

        count_df.show()

        self.logger.info(count_df.collect())

        ## while the code running you can open Spark UI
        ## http://localhost:4040/jobs/

        input("Press Enter")

        self.logger.info("Finished HelloSpark")
        self.spark.stop()


# if __name__ == "__main__":
#     myapp = HelloSQLApp("spark.conf")
#     myapp.show_count_by_country()
