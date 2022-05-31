package sparklyr

import java.io._
import java.util.Arrays

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkEnv, SparkException}

object DFUtils {
  def zipDataFrames(sc: SparkContext, df1: DataFrame, df2: DataFrame) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    sqlContext.createDataFrame(
      df1.rdd.zip(df2.rdd).map { case (r1, r2) => Row.merge(r1, r2) },
      StructType(df1.schema ++ df2.schema))
  }
}
