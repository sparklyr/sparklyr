package sparklyr

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object StructColumnUtils {
  def parseJsonColumns(
    df: DataFrame,
    jsonColumns: Array[String]
  ): Dataset[Row] = {
    // parse JSON strings of any column of df listed in jsonColumns, converting
    // them into Spark SQL StructTypes, assuming all JSON strings follow the same
    // schema
    if (df.rdd.isEmpty) {
      df
    } else {
      val sparkSession = SparkSession.builder.getOrCreate()
      import sparkSession.implicits._
      df.select(
        df.columns.map(x => {
          val col_name_quoted = "`" + x + "`"
          if (jsonColumns.contains(x)) {
           val schema = schema_of_json(lit(df.select(x).as[String].first))
           from_json(col(col_name_quoted), schema).as(x)
          } else {
            col(col_name_quoted)
          }
        }):_*
      )
    }
  }
}
