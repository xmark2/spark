# from .sp_01_hello.HelloSpark import MySparkApp
from sp_01_hello import MySparkApp
from pathlib import Path

if __name__ == "__main__":
    myapp = MySparkApp("spark.conf")
    myapp.show_count_by_country()
