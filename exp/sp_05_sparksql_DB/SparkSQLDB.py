import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, lit
from .lib import Log4j, get_spark_app_config, load_parquet_df


class SparkSQLDBApp:

    def __init__(self, path_conf, dbname):
        conf = get_spark_app_config(path_conf)

        self.spark = SparkSession.builder \
            .config(conf=conf) \
            .enableHiveSupport() \
            .getOrCreate()

        self.dbname = dbname
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {dbname}")
        self.spark.catalog.setCurrentDatabase(f"{dbname}")

        self.logger = Log4j(self.spark)

    def check_arg_data(self):
        if len(sys.argv) != 2:
            self.logger.error("Usage: HelloSpark <filename>")
            sys.exit(-1)

    # def create_sparkdb(self, path_parquet, dbname):
    def load_parquet_sparkdb(self, path_parquet, tblname):
        # self.check_arg_data()

        self.logger.info("Starting to read Parquet and Creating SparkSQLDB")

        # flighttime_csv_df = load_csv_df(self.spark, "data/flight*.csv")
        flighttime_df = load_parquet_df(self.spark, path_parquet)

        # flighttime_df.show()

        # dbname = "AIRLINE_DB"
        # self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {dbname}")
        # self.spark.catalog.setCurrentDatabase(f"{dbname}")

        self.logger.info("Parquet Schema:" + flighttime_df.schema.simpleString())
        self.logger.info(flighttime_df.printSchema())

        ## while the code running you can open Spark UI
        ## http://localhost:4040/jobs/

        input("Press Enter")

        flighttime_df.write \
            .format("csv") \
            .mode("overwrite") \
            .bucketBy(5, "ORIGIN", "OP_CARRIER") \
            .sortBy("ORIGIN", "OP_CARRIER") \
            .saveAsTable(f"{tblname}")

        self.logger.info("Finished DataSinkApp to read Parquet")
        self.spark.stop()

    def show_tables(self):
        print(self.spark.catalog.listTables(f"{self.dbname}"))

    def read_sparksql_table(self, sql_cmd):

        df_output = self.spark.sql(f"{sql_cmd}")
        df_output.show()


if __name__ == "__main__":
    myapp = SparkSQLDBApp(path_conf="spark.conf", dbname="AIRLINE_DB")
    # myapp.load_parquet_sparkdb(path_parquet="dataSource/flight*.parquet", tblname="flight_data_tbl")
    myapp.show_tables()
    myapp.read_sparksql_table(sql_cmd="select * from flight_data_tbl")

    # myapp = DataSinkApp(path_conf="spark.conf")
    # myapp.read_datasink(filter_origin='BHM', filter_carrier='HP')

# from pyspark.sql import *
#
# from lib.logger import Log4j
#
# if __name__ == "__main__":
#     spark = SparkSession \
#         .builder \
#         .master("local[3]") \
#         .appName("SparkSQLTableDemo") \
#         .enableHiveSupport() \
#         .getOrCreate()
#
#     logger = Log4j(spark)
#
#     flightTimeParquetDF = spark.read \
#         .format("parquet") \
#         .load("dataSource/")
#
#     dbname = "AIRLINE_DB"
#     spark.sql(f"CREATE DATABASE IF NOT EXISTS {dbname}")
#     spark.catalog.setCurrentDatabase(f"{dbname}")
#
#     # spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
#     # spark.catalog.setCurrentDatabase("AIRLINE_DB")
#
#     flightTimeParquetDF.write \
#         .format("csv") \
#         .mode("overwrite") \
#         .bucketBy(5, "ORIGIN", "OP_CARRIER") \
#         .sortBy("ORIGIN", "OP_CARRIER") \
#         .saveAsTable("flight_data_tbl")
#
#     # flightTimeParquetDF.write \
#     #     .format("csv") \
#     #     .mode("overwrite") \
#     #     .bucketBy(5, "ORIGIN", "OP_CARRIER") \
#     #     .saveAsTable("flight_data_tbl")
#
#     # flightTimeParquetDF.write \
#     #     .mode("overwrite") \
#     #     .partitionBy("ORIGIN", "OP_CARRIER") \
#     #     .saveAsTable("flight_data_tbl")
#
#     # flightTimeParquetDF.write \
#     #     .mode("overwrite") \
#     #     .saveAsTable("AIRLINE_DB.flight_data_tbl")
#
#     logger.info(spark.catalog.listTables("AIRLINE_DB"))
