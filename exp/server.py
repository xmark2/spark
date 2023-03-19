from sp_01_hello.HelloSpark import HelloApp
from sp_02_sparksql.HelloSparkSQL import HelloSQLApp
from pathlib import Path

if __name__ == "__main__":
    path_conf = Path().joinpath("sp_01_hello", "spark.conf")
    myapp = HelloApp(path_conf)
    myapp.show_count_by_country()

    path_conf = Path().joinpath("sp_02_sparksql", "spark.conf")
    myapp = HelloSQLApp(path_conf)
    myapp.show_count_by_country()
