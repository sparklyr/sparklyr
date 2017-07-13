package sparklyr

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object ApplyUtils {
  def groupBy(rdd: RDD[Row], colPosition: Int): RDD[Row] = {
    rdd.map(r => (r.get(colPosition), r.toSeq)).groupBy(r => r._1).map(
      r => Row(r._2.map(x => Row.fromSeq(x._2)).toSeq)
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
