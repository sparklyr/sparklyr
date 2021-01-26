package sparklyr

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TemporalArrayColumnUtils {
  def transformTemporalArrayColumns(
    df: DataFrame,
    dateColumns: Array[String],
    timestampColumns: Array[String]
  ): Dataset[Row] = {
    // ensure columns containing arrays of temporal values are casted into
    // arrays of dates and timestamps in Spark SQL
    if (df.rdd.isEmpty) {
      df
    } else {
      val sparkSession = SparkSession.builder.getOrCreate()
      import sparkSession.implicits._
      df.select(
        df.columns.map(x => {
          val col_name_quoted = "`" + x + "`"
          if (dateColumns.contains(x)) {
            transform(col(col_name_quoted), v => v.cast(DateType)).as(x)
          } else if (timestampColumns.contains(x)) {
            transform(col(col_name_quoted), v => v.cast(TimestampType)).as(x)
          } else {
            col(col_name_quoted)
          }
        }):_*
      )
    }
  }
}
