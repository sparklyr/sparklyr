# To compile the inst/java classes, run this script from the script directory

mkdir ../inst/java
scalac rspark.scala
jar cf ../inst/java/rspark_utils.jar utils.class utils$.class
