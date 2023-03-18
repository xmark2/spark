# from .sp_01_hello.HelloSpark import MySparkApp
from sp_01_hello.HelloSpark import HelloApp
from sp_01_hello.lib.utils import get_spark_app_config
from pyspark.sql import SparkSession
from sp_01_hello.lib import Log4j
from pathlib import Path


class MySparkApp:
    def __init__(self):
        # self.spark = spark
        conf = get_spark_app_config()

        self.spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

        self.logger = Log4j(self.spark)

        self.hello = HelloApp(logger=self.logger, spark=self.spark)


if __name__ == "__main__":
    myapp = MySparkApp()
    myapp.hello.show_count_by_country()
