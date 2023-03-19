from sp_01_hello.HelloSpark import HelloApp
from sp_02_sparksql.HelloSparkSQL import HelloSQLApp
from sp_03_sparkschema.SparkSchema import SparkSchemaApp
from pathlib import Path

if __name__ == "__main__":
    # path_conf = Path().joinpath("sp_01_hello", "spark.conf")
    # myapp = HelloApp(path_conf)
    # myapp.show_count_by_country()


    # path_conf = Path("sp_02_sparksql").joinpath("spark.conf")
    # path_data = Path("sp_02_sparksql").joinpath("data/sample.csv")
    # myapp = HelloSQLApp(path_conf, str(path_data))
    # myapp.show_count_by_country()


    path_conf = Path("sp_03_sparkschema").joinpath("spark.conf")
    path_data = Path("sp_03_sparkschema").joinpath("data/flight*.csv")
    myapp = SparkSchemaApp(path_conf, str(path_data))
    myapp.show_data_csv()
