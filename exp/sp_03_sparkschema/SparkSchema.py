import sys
from pyspark.sql import SparkSession
from .lib import Log4j, get_spark_app_config, load_csv_df, load_json_df


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
#
# if __name__ == "__main__":
#     spark = SparkSession \
#         .builder \
#         .master("local[3]") \
#         .appName("SparkSchemaDemo") \
#         .getOrCreate()
#
#     logger = Log4j(spark)
#
#     flightSchemaStruct = StructType([
#         StructField("FL_DATE", DateType()),
#         StructField("OP_CARRIER", StringType()),
#         StructField("OP_CARRIER_FL_NUM", IntegerType()),
#         StructField("ORIGIN", StringType()),
#         StructField("ORIGIN_CITY_NAME", StringType()),
#         StructField("DEST", StringType()),
#         StructField("DEST_CITY_NAME", StringType()),
#         StructField("CRS_DEP_TIME", IntegerType()),
#         StructField("DEP_TIME", IntegerType()),
#         StructField("WHEELS_ON", IntegerType()),
#         StructField("TAXI_IN", IntegerType()),
#         StructField("CRS_ARR_TIME", IntegerType()),
#         StructField("ARR_TIME", IntegerType()),
#         StructField("CANCELLED", IntegerType()),
#         StructField("DISTANCE", IntegerType())
#     ])
#
#
#     flightTimeCsvDF = spark.read \
#         .format("csv") \
#         .option("header", "true") \
#         .schema(flightSchemaStruct) \
#         .option("mode", "FAILFAST") \
#         .option("dateFormat", "M/d/y") \
#         .load("data/flight*.csv")
#
#     flightTimeCsvDF.show(5)
#     logger.info("CSV Schema:" + flightTimeCsvDF.schema.simpleString())
#
#     flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING,
#               ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT,
#               WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""
#
#     flightTimeJsonDF = spark.read \
#         .format("json") \
#         .schema(flightSchemaDDL) \
#         .option("dateFormat", "M/d/y") \
#         .load("data/flight*.json")
#
#     flightTimeJsonDF.show(5)
#     logger.info("JSON Schema:" + flightTimeJsonDF.schema.simpleString())
#
#     flightTimeParquetDF = spark.read \
#         .format("parquet") \
#         .load("data/flight*.parquet")
#
#     flightTimeParquetDF.show(5)
#     logger.info("Parquet Schema:" + flightTimeParquetDF.schema.simpleString())
