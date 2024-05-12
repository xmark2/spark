# from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

import sys
from pyspark.sql import SparkSession
from apache_spark3_exp.apps.lib import Log4j, get_spark_app_config


# load_survey_df, count_by_country


def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(fld, fmt))


class RowApp:

    def __init__(self, path_conf, app_id):
        conf = get_spark_app_config(path_conf, app_id)

        self.spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

        self.logger = Log4j(self.spark)

    @staticmethod
    def get_schema():
        return StructType([
            StructField("ID", StringType()),
            StructField("EventDate", StringType())
        ])

    @staticmethod
    def get_rows():
        return [Row("123", "04/05/2020"),
                Row("124", "4/5/2020"),
                Row("125", "04/5/2020"),
                Row("126", "4/05/2020")]

    def get_row_df(self):
        my_rdd = self.spark.sparkContext.parallelize(self.get_rows(), 2)
        return self.spark.createDataFrame(my_rdd, self.get_schema())

    def get_row_df_todate(self):
        return to_date_df(self.get_row_df(), "M/d/y", "EventDate")


if __name__ == "__main__":
    myapp = RowApp(path_conf="../sp_06_RowDemo/spark.conf")

    myapp.get_row_df().printSchema()
    myapp.get_row_df().show()

    myapp.get_row_df_todate().printSchema()
    myapp.get_row_df_todate().show()


# if __name__ == "__main__":
#     spark = SparkSession \
#         .builder \
#         .master("local[3]") \
#         .appName("RowDemo") \
#         .getOrCreate()
#
#     logger = Log4j(spark)
#
#     my_schema = \
#         StructType([
#             StructField("ID", StringType()),
#             StructField("EventDate", StringType())
#         ])
#
#     my_rows = [Row("123", "04/05/2020"),
#                Row("124", "4/5/2020"),
#                Row("125", "04/5/2020"),
#                Row("126", "4/05/2020")]
#
#     my_rdd = spark.sparkContext.parallelize(my_rows, 2)
#     my_df = spark.createDataFrame(my_rdd, my_schema)
#
#     my_df.printSchema()
#     my_df.show()
#     new_df = to_date_df(my_df, "M/d/y", "EventDate")
#     new_df.printSchema()
#     new_df.show()
