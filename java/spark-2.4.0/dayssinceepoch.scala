package sparklyr

// Special wrapper class for date value collected from a Spark dataframe
// (where `value` is derived from applying the transformations in
//  org.apache.spark.sql.catalyst.util.DateTimeUtils.fromJavaDate
//  to the java.sql.Date value)
case class DaysSinceEpoch(value: Option[Int])
