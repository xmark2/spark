#!/bin/bash

current_date=$(date +%Y-%m-%d__%H:%M:%S)
logfile=$(pwd)/"$0".log

java --version

if [ $? -ne 0 ]; then
  for package in default-jre openjdk-11-jre-headless;
  do
    sudo apt install $package
    echo "$current_date -- The installation of $package was successful.">>$logfile
    echo "The new command is available here:"
    which $package
  done

  java version
fi

### Remove java jdk
#sudo apt-get purge openjdk*

### set up JAVA_HOME environment variable
#sudo update-alternatives --config java
#sudo nano /etc/environment