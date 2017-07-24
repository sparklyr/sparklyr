package sparklyr

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object ApplyUtils {
  def groupBy(rdd: RDD[Row], colPosition: Array[Int]): RDD[Row] = {
    rdd.groupBy(r => r.get(colPosition(0))).map(
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
