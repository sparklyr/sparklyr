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
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkEnv, SparkException}

object Utils {

  var rPackages: Option[String] = None

  /**
   * Return a nice string representation of the exception. It will call "printStackTrace" to
   * recursively generate the stack trace including the exception and its causes.
   */
  def exceptionString(e: Throwable): String = {
    if (e == null) {
      "No exception information provided."
    } else {
      // Use e.printStackTrace here because e.getStackTrace doesn't include the cause
      val stringWriter = new StringWriter()
      e.printStackTrace(new PrintWriter(stringWriter))
      stringWriter.toString
    }
  }

  /**
   * Utilities for collecting columns / Datasets back to R
   */

  def collectColumnBoolean(df: DataFrame, colName: String): Array[Boolean] = {
    df.select(colName).rdd.map(row => row(0).asInstanceOf[Boolean]).collect()
  }

  def collectColumnInteger(df: DataFrame, colName: String): Array[Int] = {
    df.select(colName).rdd.map(row => {
       val element = row(0)
       if (element.isInstanceOf[Int]) element.asInstanceOf[Int] else scala.Int.MinValue
    }).collect()
  }

  def collectColumnDouble(df: DataFrame, colName: String): Array[Double] = {
    df.select(colName).rdd.map(row => {
       val element = row(0)
       if (element.isInstanceOf[Double]) element.asInstanceOf[Double] else scala.Double.NaN
    }).collect()
  }

  def collectColumnString(df: DataFrame, colName: String): String = {
    val text = df.select(colName).rdd.map(row => {
      val element = row(0)
      if (element.isInstanceOf[String]) element.asInstanceOf[String] else "<NA>"
    }).collect().mkString("\n")

    if (text.length() > 0) text + "\n" else text
  }

  def collectColumnDefault(df: DataFrame, colName: String): Array[Any] = {
    df.select(colName).rdd.map(row => row(0)).collect()
  }

  def collectColumn(df: DataFrame, colName: String, colType: String) = {
    colType match {
      case "BooleanType" => collectColumnBoolean(df, colName)
      case "IntegerType" => collectColumnInteger(df, colName)
      case "DoubleType"  => collectColumnDouble(df, colName)
      case "StringType"  => collectColumnString(df, colName)
      case _             => collectColumnDefault(df, colName)
    }
  }

  def collectImplBoolean(local: Array[Row], idx: Integer) = {
    local.map{row => row(idx).asInstanceOf[Boolean]}
  }

  def collectImplInteger(local: Array[Row], idx: Integer) = {
    local.map{row => {
      val el = row(idx)
      if (el.isInstanceOf[Int]) el.asInstanceOf[Int] else scala.Int.MinValue
    }}
  }

  def collectImplDouble(local: Array[Row], idx: Integer) = {
    local.map{row => {
      val el = row(idx)
      if (el.isInstanceOf[Double]) el.asInstanceOf[Double] else scala.Double.NaN
    }}
  }

  def collectImplLong(local: Array[Row], idx: Integer) = {
    local.map{row => {
      val el = row(idx)
      if (el.isInstanceOf[Long]) el.asInstanceOf[Long] else scala.Long.MinValue
    }}
  }

  def collectImplByte(local: Array[Row], idx: Integer) = {
    local.map{row => {
      val el = row(idx)
      if (el.isInstanceOf[Byte]) el.asInstanceOf[Byte] else scala.Byte.MinValue
    }}
  }

  def collectImplFloat(local: Array[Row], idx: Integer) = {
    local.map{row => {
      val el = row(idx)
      if (el.isInstanceOf[Float]) el.asInstanceOf[Float] else scala.Float.MinValue
    }}
  }

  def collectImplShort(local: Array[Row], idx: Integer) = {
    local.map{row => {
      val el = row(idx)
      if (el.isInstanceOf[Short]) el.asInstanceOf[Short] else scala.Short.MinValue
    }}
  }

  def collectImplForceString(local: Array[Row], idx: Integer) = {
    var text = local.map{row => {
      val el = row(idx)
      if (el != null) el.toString() else "<NA>"
    }}.mkString("\n")

    if (text.length() > 0) text + "\n" else text
  }

  def collectImplString(local: Array[Row], idx: Integer) = {
    var text = local.map{row => {
      val el = row(idx)
      if (el.isInstanceOf[String]) el.asInstanceOf[String] else "<NA>"
    }}.mkString("\n")

    if (text.length() > 0) text + "\n" else text
  }

  def collectImplDecimal(local: Array[Row], idx: Integer) = {
    local.map{row => {
      val el = row(idx)
      if (el.isInstanceOf[java.math.BigDecimal])
        el.asInstanceOf[java.math.BigDecimal].doubleValue
      else
        scala.Double.NaN
    }}
  }

  def collectImplVector(local: Array[Row], idx: Integer) = {
    local.map{row => {
      val el = row(idx)
      el match {
        case null => Array.empty
        case _    => el.getClass.getDeclaredMethod("toArray").invoke(el)
      }
    }}
  }

  def collectImplDefault(local: Array[Row], idx: Integer) = {
    local.map(row => row(idx))
  }

  def collectImpl(local: Array[Row], idx: Integer, colType: String) = {

    val ReDecimalType = "(DecimalType.*)".r
    val ReVectorType  = "(.*VectorUDT.*)".r

    colType match {
      case "BooleanType"          => collectImplBoolean(local, idx)
      case "IntegerType"          => collectImplInteger(local, idx)
      case "DoubleType"           => collectImplDouble(local, idx)
      case "StringType"           => collectImplString(local, idx)
      case "LongType"             => collectImplLong(local, idx)

      case "ByteType"             => collectImplByte(local, idx)
      case "FloatType"            => collectImplFloat(local, idx)
      case "ShortType"            => collectImplShort(local, idx)
      case "Decimal"              => collectImplForceString(local, idx)

      case "TimestampType"        => collectImplForceString(local, idx)
      case "CalendarIntervalType" => collectImplForceString(local, idx)
      case "DateType"             => collectImplForceString(local, idx)

      case ReDecimalType(_)       => collectImplDecimal(local, idx)
      case ReVectorType(_)        => collectImplVector(local, idx)

      case _                      => collectImplDefault(local, idx)
    }
  }

  def collect(df: DataFrame): Array[_] = {
    val local : Array[Row] = df.collect()
    val dtypes = df.dtypes
    (0 until dtypes.length).map{i => collectImpl(local, i, dtypes(i)._2)}.toArray
  }

  def createDataFrame(sc: SparkContext, rows: Array[_], partitions: Int): RDD[Row] = {
    var data = rows.map(o => {
      val r = o.asInstanceOf[Array[_]]
      org.apache.spark.sql.Row.fromSeq(r)
    })

    sc.parallelize(data, partitions)
  }

  def createDataFrameFromText(
    sc: SparkContext,
    rows: Array[String],
    columns: Array[String],
    partitions: Int,
    separator: String): RDD[Row] = {

    var data = rows.map(o => {
      val r = o.split(separator, -1)
      var typed = (Array.range(0, r.length)).map(idx => {
        val column = columns(idx)
        val value = r(idx)

        column match {
          case "integer"  => if (Try(value.toInt).isSuccess) value.toInt else null.asInstanceOf[Int]
          case "double"  => if (Try(value.toDouble).isSuccess) value.toDouble else null.asInstanceOf[Double]
          case "logical" => if (Try(value.toBoolean).isSuccess) value.toBoolean else null.asInstanceOf[Boolean]
          case _ => value
        }
      })

      org.apache.spark.sql.Row.fromSeq(typed)
    })

    sc.parallelize(data, partitions)
  }

  def classExists(name: String): Boolean = {
    scala.util.Try(Class.forName(name)).isSuccess
  }
}
