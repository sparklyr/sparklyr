package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

object SQLUtils {
  def createDataFrame(
    sc: org.apache.spark.sql.SparkSession,
    catalystRows: RDD[InternalRow],
    schema: StructType
  ): DataFrame = {
    sc.internalCreateDataFrame(catalystRows, schema)
  }
}
