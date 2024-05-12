from apache_spark3_exp.apps import HelloApp, SparkSqlApp, SparkSchemaApp, DataSinkApp, SparkSQLDBApp, RowApp
from pathlib import Path

path_conf = Path().joinpath("spark.conf")


def execute(run_id=None):
    if not run_id:
        return

    if '01' in run_id:
        app1 = HelloApp(path_conf=path_conf, app_id='01')
        app1.show_count_by_country()

    if '02' in run_id:
        path_data = Path().joinpath("data/sample.csv")
        app2 = SparkSqlApp(path_conf=path_conf, app_id='02', path_data=str(path_data))
        app2.show_count_by_country()

    if '03csv' in run_id:
        path_csv_data = Path().joinpath("data/flight*.csv")
        app3 = SparkSchemaApp(path_conf=path_conf, app_id='03')
        app3.show_data_csv(str(path_csv_data))

    if '03json' in run_id:
        path_json_data = Path().joinpath("data/flight*.json")
        app3 = SparkSchemaApp(path_conf=path_conf, app_id='03')
        app3.show_data_json(str(path_json_data))

    if '03par' in run_id:
        path_parquet_data = Path().joinpath("data/flight*.parquet")
        app3 = SparkSchemaApp(path_conf=path_conf, app_id='03')
        app3.show_data_parquet(str(path_parquet_data))

    if '04sink' in run_id:
        app4 = DataSinkApp(path_conf=path_conf, app_id='04')
        app4.get_datasink(path_parquet="data/flight*.parquet")

    if '04read' in run_id:
        app4 = DataSinkApp(path_conf=path_conf, app_id='04')
        app4.read_data_sink(filter_carrier='AA', filter_origin='BHM')

    if '05sql_db' in run_id:
        app5 = SparkSQLDBApp(path_conf=path_conf, app_id='05', dbname="AIRLINE_DB")
        app5.load_parquet_sparkdb(path_parquet="data/flight*.parquet",
                                  tblname="flight_data_tbl")

    if '05sql_read' in run_id:
        app5 = SparkSQLDBApp(path_conf=path_conf, app_id='05', dbname="AIRLINE_DB")
        app5.read_sparksql_table(sql_cmd="select * from flight_data_tbl")

    if '06row' in run_id:
        app6 = RowApp(path_conf=path_conf, app_id='06')
        app6.get_row_df().printSchema()
        app6.get_row_df().show()

        app6.get_row_df_todate().printSchema()
        app6.get_row_df_todate().show()


if __name__ == "__main__":
    execute(run_id=[
        # '01', '02', '03csv', '03json', '03par'
        # '04sink'
        # '04read'
        # '05sql_db'
        # '05sql_read'
        '06row'
    ])

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
