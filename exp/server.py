from sp_01_hello.HelloSpark import HelloApp
from sp_02_sparksql.HelloSparkSQL import SparkSqlApp
from sp_03_sparkschema.SparkSchema import SparkSchemaApp
from sp_04_datasink.DataSink import DataSinkApp
from sp_05_sparksql_DB.SparkSQLDB import SparkSQLDBApp
from sp_06_RowDemo.RowDemo import RowApp
from pathlib import Path

if __name__ == "__main__":
    # path_conf = Path().joinpath("sp_01_hello", "spark.conf")
    # myapp = HelloApp(path_conf)
    # myapp.show_count_by_country()
    #
    # path_conf = Path().joinpath("sp_02_sparksql", "spark.conf")
    # path_data = Path("sp_02_sparksql").joinpath("data/sample.csv")
    # myapp = SparkSqlApp(path_conf, str(path_data))
    # myapp.show_count_by_country()
    #
    # path_conf = Path().joinpath("sp_03_sparkschema", "spark.conf")
    # path_csv_data = Path("sp_03_sparkschema").joinpath("data/flight*.csv")
    # path_json_data = Path("sp_03_sparkschema").joinpath("data/flight*.json")
    # path_parquet_data = Path("sp_03_sparkschema").joinpath("data/flight*.parquet")
    # myapp = SparkSchemaApp(path_conf)
    # myapp.show_data_csv(str(path_csv_data))
    # myapp = SparkSchemaApp(path_conf)
    # myapp.show_data_json(str(path_json_data))
    # myapp = SparkSchemaApp(path_conf)
    # myapp.show_data_parquet(str(path_parquet_data))

    # path_conf = Path().joinpath("sp_04_datasink", "spark.conf")
    # myapp = DataSinkApp(path_conf)
    # myapp.get_datasink(path_parquet="sp_04_datasink/dataSource/flight*.parquet")
    #
    # myapp = DataSinkApp(path_conf)
    # myapp.read_datasink(filter_origin='BHM', filter_carrier='HP')

    # path_conf = Path().joinpath("sp_05_sparksql_DB", "spark.conf")
    # myapp = SparkSQLDBApp(path_conf, dbname="AIRLINE_DB")
    # myapp.load_parquet_sparkdb(path_parquet="sp_05_sparksql_DB/dataSource/flight*.parquet",
    #                            tblname="flight_data_tbl")
    #
    # myapp = SparkSQLDBApp(path_conf, dbname="AIRLINE_DB")
    # myapp.read_sparksql_table(sql_cmd="select * from flight_data_tbl")

    path_conf = Path().joinpath("sp_06_RowDemo", "spark.conf")
    myapp = RowApp(path_conf)
    myapp.get_row_df().printSchema()
    myapp.get_row_df().show()

    myapp.get_row_df_todate().printSchema()
    myapp.get_row_df_todate().show()
