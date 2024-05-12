from apache_spark3_exp.apps import HelloApp, SparkSqlApp, SparkSchemaApp
from pathlib import Path

path_conf = Path().joinpath("spark.conf")


class SparkRunner:
    def __init__(self):
        self.app1 = HelloApp(path_conf=path_conf, app_id='01')

        path_data = Path().joinpath("data/sample.csv")
        self.app2 = SparkSqlApp(path_conf=path_conf, app_id='02', path_data=str(path_data))

        self.app3 = SparkSchemaApp(path_conf=path_conf, app_id='03')

    def execute(self, run_id='Default'):
        if run_id == 'Default':
            return

        if run_id == '01':
            self.app1.show_count_by_country()

        if run_id == '02':
            self.app2.show_count_by_country()

        if run_id == '03csv':
            path_csv_data = Path().joinpath("data/flight*.csv")
            self.app3.show_data_csv(str(path_csv_data))

        if run_id == '03json':
            path_json_data = Path().joinpath("data/flight*.json")
            self.app3.show_data_json(str(path_json_data))

        if run_id == '03par':
            path_parquet_data = Path().joinpath("data/flight*.parquet")
            self.app3.show_data_parquet(str(path_parquet_data))


if __name__ == "__main__":

    SparkRunner().execute(run_id='03json')
    # exec_app1()
    # exec_app2()
    # exec_app3_csv()


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

    # path_conf = Path().joinpath("sp_06_RowDemo", "spark.conf")
    # myapp = RowApp(path_conf)
    # myapp.get_row_df().printSchema()
    # myapp.get_row_df().show()
    #
    # myapp.get_row_df_todate().printSchema()
    # myapp.get_row_df_todate().show()
