import sys
from pyspark.sql import SparkSession
from .lib import Log4j, get_spark_app_config, load_csv_df, load_json_df, load_parquet_df


class SparkSchemaApp:

    def __init__(self, path_conf):

        conf = get_spark_app_config(path_conf)

        self.spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

        self.logger = Log4j(self.spark)

    def check_arg_data(self):
        if len(sys.argv) != 2:
            self.logger.error("Usage: HelloSpark <filename>")
            sys.exit(-1)

    def show_data_csv(self, path_csv):

        self.check_arg_data()

        self.logger.info("Starting SparkSchemaApp CSV")

        # flighttime_csv_df = load_csv_df(self.spark, "data/flight*.csv")
        flighttime_df = load_csv_df(self.spark, path_csv)

        flighttime_df.show()

        self.logger.info("CSV Schema:" + flighttime_df.schema.simpleString())

        ## while the code running you can open Spark UI
        ## http://localhost:4040/jobs/

        input("Press Enter")

        self.logger.info("Finished SparkSchemaApp CSV")
        self.spark.stop()

    def show_data_json(self, path_json):

        self.check_arg_data()

        self.logger.info("Starting SparkSchemaApp JSON")

        # flighttime_csv_df = load_csv_df(self.spark, "data/flight*.csv")
        flighttime_df = load_json_df(self.spark, path_json)

        flighttime_df.show()

        self.logger.info("JSON Schema:" + flighttime_df.schema.simpleString())

        ## while the code running you can open Spark UI
        ## http://localhost:4040/jobs/

        input("Press Enter")

        self.logger.info("Finished SparkSchemaApp JSON")
        self.spark.stop()

    def show_data_parquet(self, path_parquet):

        self.check_arg_data()

        self.logger.info("Starting SparkSchemaApp Parquet")

        # flighttime_csv_df = load_csv_df(self.spark, "data/flight*.csv")
        flighttime_df = load_parquet_df(self.spark, path_parquet)

        flighttime_df.show()

        self.logger.info("Parquet Schema:" + flighttime_df.schema.simpleString())

        ## while the code running you can open Spark UI
        ## http://localhost:4040/jobs/

        input("Press Enter")

        self.logger.info("Finished SparkSchemaApp Parquet")
        self.spark.stop()
