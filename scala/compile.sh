#!/bin/sh
# To compile the inst/java classes, run this script from the script directory

THE_CLASSPATH=.
for i in `ls ~/Library/Caches/spark/spark-2.0.0-preview-bin-hadoop2.6/jars/*.jar`
do
  THE_CLASSPATH=${THE_CLASSPATH}:${i}
done

mkdir ../inst/java
scalac -classpath "$THE_CLASSPATH" rspark.scala
jar cf ../inst/java/rspark_utils.jar utils.class utils$.class
