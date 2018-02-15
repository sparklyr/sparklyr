//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object ApplyUtils {
  def groupBy(rdd: RDD[Row], colPosition: Array[Int]): RDD[Row] = {
    rdd.groupBy(
      r => colPosition.map(p => r.get(p)).mkString("|")
    ).map(
      r => Row(r._2.toSeq)
    )
  }

  def groupBySchema(df: DataFrame): types.StructType = {
    types.StructType(
      Array(
        types.StructField(
          "aggregate",
          types.ArrayType(
            df.schema
          )
        )
      )
    )
  }
}
