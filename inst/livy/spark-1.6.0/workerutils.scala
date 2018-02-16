//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

import java.io._
import java.util.Arrays

import scala.util.Try

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object WorkerUtils {

  def buildStructTypeForLongField(): StructType = {
    val fields = Array(StructField("id", LongType, false))
    StructType(fields)
  }

  def mapRddLongToRddRow(rdd: RDD[Long]): RDD[Row] = {
    rdd.map(x => org.apache.spark.sql.Row(x))
  }

}
