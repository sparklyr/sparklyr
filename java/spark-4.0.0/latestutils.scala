package sparklyr

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object LatestUtils {
  def convertToArray(data: Dataset[Row]): Dataset[Row] = {
    // Use Spark SQL functions instead of map()
    data.select(struct(data.columns.map(col): _*).alias("array"))
  }
}
