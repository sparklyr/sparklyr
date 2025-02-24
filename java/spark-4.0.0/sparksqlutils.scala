package sparklyr

import java.io._
import java.net.{InetAddress, ServerSocket}
import java.util.Arrays

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

object SQLUtils2 {
  def createDataFrame(
    sc: org.apache.spark.sql.SparkSession,
    catalystRows: RDD[InternalRow],
    schema: StructType
  ): DataFrame = {
    val sdf = sc.internalCreateDataFrame(catalystRows, schema)

    sdf
  }
}
