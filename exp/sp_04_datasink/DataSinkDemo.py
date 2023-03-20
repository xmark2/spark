from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id, lit

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    flightTimeParquetDF.createOrReplaceTempView("flight_data")

    print(flightTimeParquetDF.columns)

    logger.info("start sql")
    spark.sql("select count(*) from flight_data where OP_CARRIER='AA' and ORIGIN='BHM'").show()
    logger.info("finished sql")

    # logger.info("Num Partitions before: " + str(flightTimeParquetDF.rdd.getNumPartitions()))
    # flightTimeParquetDF.groupBy(spark_partition_id()).count().show()
    #
    # partitionedDF = flightTimeParquetDF.repartition(5)
    # logger.info("Num Partitions after: " + str(partitionedDF.rdd.getNumPartitions()))
    # partitionedDF.groupBy(spark_partition_id()).count().show()

    # partitionedDF.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "dataSink/avro/") \
    #     .save()

    # flightTimeParquetDF.write \
    #     .format("json") \
    #     .mode("overwrite") \
    #     .option("path", "dataSink/json/") \
    #     .partitionBy("OP_CARRIER", "ORIGIN") \
    #     .option("maxRecordsPerFile", 10000) \
    #     .save()
    #
    filter_carrier = 'AA'
    filter_origin = 'BHM'

    path = f"dataSink/json/OP_CARRIER={filter_carrier}/ORIGIN={filter_origin}/part*"
    # newcol = path.split("/")[-1].split("=")
    # print(newcol)
    #
    dfjson = spark.read \
        .format("json") \
        .option("mode", "FAILFAST") \
        .load(path)\
        .withColumn('OP_CARRIER', lit(filter_carrier))\
        .withColumn('ORIGIN', lit(filter_origin))

    dfjson.createOrReplaceTempView("flight_data2")

    # print(flightTimeParquetDF.columns)

    logger.info("start sql")
    df = spark.sql("select * from flight_data2")
    print(df.columns)
    df.show()
    logger.info("finished sql")
