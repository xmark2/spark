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

#### Databricks

* Register Community Edition
* [Databricks Quick Start](https://docs.databricks.com/getting-started/quick-start.html)

#### Anaconda

* [steps](https://itslinuxfoss.com/install-anaconda-ubuntu-22-04/) 




### Spark Workshop

* https://github.com/PacktWorkshops/The-Spark-Workshop