# spark

### Spark Programming in Python for Beginners with Apache Spark 3

#### Install

* [java jdk](https://www.digitalocean.com/community/tutorials/how-to-install-java-with-apt-on-ubuntu-22-04)
* [spark ubuntu 22.04](https://dev.to/kinyungu_denis/to-install-apache-spark-and-run-pyspark-in-ubuntu-2204-4i79)
* [spark ubuntu 20.04](https://cloudinfrastructureservices.co.uk/how-to-install-apache-spark-on-ubuntu-20-04/)

Or use ./install bash script apache_spark3/00-installation folder

#### Codes

* [LearningJournal Github repo](https://github.com/LearningJournal/Spark-Programming-In-Python)

#### how-to-permanently-set-an-environment-variable

* Open the file for editing with
* gedit ~/.profile
* Add the command to the bottom of the file.
* Save and close gedit.
* Log out and log in again.

#### Log

Change template log to conf 

`cd /mnt/spark/conf`

`mv spark-defaults.conf.template spark-defaults.conf`

`nano spark-defaults.conf`

add the following

`spark.driver.extraJavaOptions      -Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=hello-spark`
`spark.jars.packages                org.apache.spark:spark-avro_2.12:3.5.1`

#### Databricks

* Register Community Edition
* [Databricks Quick Start](https://docs.databricks.com/getting-started/quick-start.html)

#### Anaconda

* [steps](https://itslinuxfoss.com/install-anaconda-ubuntu-22-04/) 

#### Yarn pyspark

`pyspark --master local[3] --driver-memory 2G`

`pyspark --master yarn --driver-memory 1G --executor-memory 500M --num-executor 2 --executor-cores 1`

#### Zeppelin

* [install Zeppelin ubuntu 18.04](https://www.youtube.com/watch?v=XW0zZCknjiw)

#### [DataFrameReader](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html?highlight=dataframereader)

* General structure

```
DataFrameReader
    .format(...)
    .option("key", "value")
    .schema(...)
    .load()
```

* Example

```
spark.read
    .format("csv")
    .option("header", "true")
    .option("path", "/data/mycsvfiles/")
    .option("mode", "FAILFAST")
    .schema(mySchema)
    .load()
```

[Option modes](https://www.coffeeandtips.com/post/differences-between-failfast-permissive-and-dropmalfored-modes-in-dataframes)

* `.option("mode", "PERMISSIVE")`
* `.option("mode", "DROPMALFORMED")`
* `.option("mode", "FAILFAST")`

[Spark Data Types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)

#### [DataFrameWriter](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter)

* General structure

```
DataFrameWriter
    .format(...)
    .mode(...)
    .option(...)
    .partitionBy(...)
    .bucketBy(...)
    .sortBy(...)
    .save()
```

* Example

```
spark.write
    .format("parquet")
    .mode(saveMode)
    .option("path", "/data/flights/")
    .save()
```

#### saveModes

* append
* overwrite
* errorIfExists
* ignore

### Learning Spark 2nd Edition

* https://github.com/databricks/LearningSparkV2


### Spark Workshop

* https://github.com/PacktWorkshops/The-Spark-Workshop