package sparklyr

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object StructColumnUtils {
  def parseJsonColumns(
    df: DataFrame,
    json_columns: Array[String]
  ): DataFrame = {
    // parse JSON strings of any column of df listed in json_columns, converting
    // them into Spark SQL StructTypes, assuming all JSON strings follow the same
    // schema
    if (df.rdd.isEmpty) {
      df
    } else {
      val sparkSession = SparkSession.builder.getOrCreate()
      import sparkSession.implicits._
      df.select(
        df.columns.map(x => {
          if (json_columns.contains(x)) {
           val schema = schema_of_json(lit(df.select(x).as[String].first))
           from_json(col(x), schema).as(x)
          } else {
            col(x)
          }
        }):_*
      )
    }
  }
}
