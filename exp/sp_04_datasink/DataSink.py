import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, lit
from .lib import Log4j, get_spark_app_config, load_parquet_df


class DataSinkApp:

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

    def get_datasink(self, path_parquet):

        # self.check_arg_data()

        self.logger.info("Starting DataSinkApp to read Parquet")

        # flighttime_csv_df = load_csv_df(self.spark, "data/flight*.csv")
        flighttime_df = load_parquet_df(self.spark, path_parquet)

        # flighttime_df.show()

        flighttime_df.write \
            .format("json") \
            .mode("overwrite") \
            .option("path", "dataSink/json/") \
            .partitionBy("OP_CARRIER", "ORIGIN") \
            .option("maxRecordsPerFile", 10000) \
            .save()

        self.logger.info("Parquet Schema:" + flighttime_df.schema.simpleString())

        ## while the code running you can open Spark UI
        ## http://localhost:4040/jobs/

        input("Press Enter")

        self.logger.info("Finished DataSinkApp to read Parquet")
        self.spark.stop()

    def read_datasink(self, filter_carrier, filter_origin):
        filter_carrier = 'AA'
        filter_origin = 'BHM'

        path = f"dataSink/json/OP_CARRIER={filter_carrier}/ORIGIN={filter_origin}/part*"

        df_json = self.spark.read \
            .format("json") \
            .option("mode", "FAILFAST") \
            .load(path) \
            .withColumn('OP_CARRIER', lit(filter_carrier)) \
            .withColumn('ORIGIN', lit(filter_origin))

        df_json.createOrReplaceTempView("flight_data")

        self.logger.info("start sql")
        df_output = self.spark.sql("select * from flight_data")
        print(df_output.columns)
        df_output.show()
        self.logger.info("finished sql")


if __name__ == "__main__":
    myapp = DataSinkApp(path_conf="spark.conf")
    myapp.get_datasink(path_parquet="dataSource/flight*.parquet")

    myapp = DataSinkApp(path_conf="spark.conf")
    myapp.read_datasink(filter_origin='BHM', filter_carrier='HP')

    # # partitionedDF.write \
    # #     .format("avro") \
    # #     .mode("overwrite") \
    # #     .option("path", "dataSink/avro/") \
    # #     .save()
