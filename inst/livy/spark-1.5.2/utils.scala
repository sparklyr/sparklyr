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
import org.apache.spark.{SparkEnv, SparkException}

object Utils {

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

  def collectColumnString(df: DataFrame, colName: String, separator: String): String = {
    val text = df.select(colName).rdd.map(row => {
      val element = row(0)
      if (element.isInstanceOf[String]) element.asInstanceOf[String] else "<NA>"
    }).collect().mkString(separator)

    if (text.length() > 0) text + separator else text
  }

  def collectColumnDefault(df: DataFrame, colName: String): Array[Any] = {
    df.select(colName).rdd.map(row => row(0)).collect()
  }

  def collectColumn(df: DataFrame, colName: String, colType: String, separator: String) = {
    colType match {
      case "BooleanType" => collectColumnBoolean(df, colName)
      case "IntegerType" => collectColumnInteger(df, colName)
      case "DoubleType"  => collectColumnDouble(df, colName)
      case "StringType"  => collectColumnString(df, colName, separator)
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

  def collectImplFloat(local: Array[Row], idx: Integer): Array[Double]  = {
    local.map{row => {
      val el = row(idx)
      if (el.isInstanceOf[Float]) el.asInstanceOf[Float].toDouble else scala.Double.NaN
    }}
  }

  def collectImplByte(local: Array[Row], idx: Integer): Array[Int] = {
    local.map{row => {
      val el = row(idx)
      if (el.isInstanceOf[Byte]) el.asInstanceOf[Byte].toInt else scala.Int.MinValue
    }}
  }

  def collectImplShort(local: Array[Row], idx: Integer): Array[Int] = {
    local.map{row => {
      val el = row(idx)
      if (el.isInstanceOf[Short]) el.asInstanceOf[Short].toInt else scala.Int.MinValue
    }}
  }

  def collectImplLong(local: Array[Row], idx: Integer) = {
    local.map{row => {
      val el = row(idx)
      if (el.isInstanceOf[Long]) el.asInstanceOf[Long].toDouble else scala.Double.NaN
    }}
  }

  def collectImplForceString(local: Array[Row], idx: Integer, separator: String) = {
    var text = local.map{row => {
      val el = row(idx)
      if (el != null) el.toString() else "<NA>"
    }}.mkString(separator)

    if (text.length() > 0) text + separator else text
  }

  def collectImplString(local: Array[Row], idx: Integer, separator: String) = {
    var text = local.map{row => {
      val el = row(idx)
      if (el.isInstanceOf[String]) el.asInstanceOf[String] else "<NA>"
    }}.mkString(separator)

    if (text.length() > 0) text + separator else text
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
        case _: Seq[_] => el.asInstanceOf[Seq[Any]].toArray
        case _ => el.getClass.getDeclaredMethod("toArray").invoke(el)
      }
    }}
  }

  def collectImplTimestamp(local: Array[Row], idx: Integer) = {
    local.map{row => {
      val el = row(idx)
      if (el.isInstanceOf[java.sql.Timestamp]) el.asInstanceOf[java.sql.Timestamp] else new java.sql.Timestamp(0)
    }}
  }

  def collectImplDate(local: Array[Row], idx: Integer) = {
    local.map{row => {
      val el = row(idx)
      if (el.isInstanceOf[java.sql.Date]) el.asInstanceOf[java.sql.Date] else new java.sql.Date(0)
    }}
  }

  def collectImplDefault(local: Array[Row], idx: Integer) = {
    local.map(row => row(idx))
  }

  def collectImpl(local: Array[Row], idx: Integer, colType: String, separator: String) = {

    val ReDecimalType = "(DecimalType.*)".r
    val ReVectorType  = "(.*VectorUDT.*)".r

    colType match {
      case "BooleanType"          => collectImplBoolean(local, idx)
      case "IntegerType"          => collectImplInteger(local, idx)
      case "DoubleType"           => collectImplDouble(local, idx)
      case "StringType"           => collectImplString(local, idx, separator)
      case "LongType"             => collectImplLong(local, idx)

      case "ByteType"             => collectImplByte(local, idx)
      case "FloatType"            => collectImplFloat(local, idx)
      case "ShortType"            => collectImplShort(local, idx)
      case "Decimal"              => collectImplForceString(local, idx, separator)

      case "TimestampType"        => collectImplTimestamp(local, idx)
      case "CalendarIntervalType" => collectImplForceString(local, idx, separator)
      case "DateType"             => collectImplDate(local, idx)

      case ReDecimalType(_)       => collectImplDecimal(local, idx)
      case ReVectorType(_)        => collectImplVector(local, idx)

      case "NullType"             => collectImplForceString(local, idx, separator)

      case _                      => collectImplDefault(local, idx)
    }
  }

  def collect(df: DataFrame, separator: String): Array[_] = {
    val local : Array[Row] = df.collect()
    val dtypes = df.dtypes
    (0 until dtypes.length).map{i => collectImpl(local, i, dtypes(i)._2, separator)}.toArray
  }

  def separateColumnArray(df: DataFrame,
                          column: String,
                          names: Array[String],
                          indices: Array[Int]) =
  {
    // extract columns of interest
    var col = df.apply(column)
    var colexprs = df.columns.map(df.apply(_))

    // append column expressions that separate from
    // desired column
    (0 until names.length).map{i => {
      val name = names(i)
      val index = indices(i)
      colexprs :+= col.getItem(index).as(name)
    }}

    // select with these column expressions
    df.select(colexprs: _*)
  }

  def separateColumnVector(df: DataFrame,
                           column: String,
                           names: Array[String],
                           indices: Array[Int]) =
  {
    // extract columns of interest
    var col = df.apply(column)
    var colexprs = df.columns.map(df.apply(_))

    // define a udf for extracting vector elements
    // note that we use 'Any' type here just to ensure
    // this compiles cleanly with different Spark versions
    val extractor = udf {
      (x: Any, i: Int) => {
         val el = x.getClass.getDeclaredMethod("toArray").invoke(x)
         val array = el.asInstanceOf[Array[Double]]
         array(i)
      }
    }

    // append column expressions that separate from
    // desired column
    (0 until names.length).map{i => {
      val name = names(i)
      val index = indices(i)
      colexprs :+= extractor(col, lit(index)).as(name)
    }}

    // select with these column expressions
    df.select(colexprs: _*)
  }

  def separateColumnStruct(df: DataFrame,
                          column: String,
                          names: Array[String],
                          indices: Array[Int],
                          intoIsSet: Boolean) =
  {
    // extract columns of interest
    var col = df.apply(column)
    var colexprs = df.columns.map(df.apply(_))

    val fieldNames: Array[String] = df
      .select(column)
      .schema
      .fields
      .flatMap(f => f.dataType match { case struct: StructType => struct.fields})
      .map(_.name)

    val outNames: Array[String] = if (intoIsSet) names else
      fieldNames

    // append column expressions that separate from
    // desired column
    (0 until outNames.length).map{i => {
      val name = outNames(i)
      val index = indices(i)
      colexprs :+= col.getItem(fieldNames(index)).as(name)
    }}

    // select with these column expressions
    df.select(colexprs: _*)
  }

  def separateColumn(df: DataFrame,
                     column: String,
                     names: Array[String],
                     indices: Array[Int],
                     intoIsSet: Boolean) =
  {
    // extract column of interest
    val col = df.apply(column)

    // figure out the type name for this column
    val schema = df.schema
    val typeName = schema.apply(schema.fieldIndex(column)).dataType.typeName

    // delegate to appropriate separator
    typeName match {
      case "array"  => separateColumnArray(df, column, names, indices)
      case "vector" => separateColumnVector(df, column, names, indices)
      case "struct" => separateColumnStruct(df, column, names, indices, intoIsSet)
      case _        => {
        throw new IllegalArgumentException("unhandled type '" + typeName + "'")
      }
    }
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

  def createDataFrameFromCsv(
    sc: SparkContext,
    path: String,
    columns: Array[String],
    partitions: Int,
    separator: String): RDD[Row] = {

    val lines = scala.io.Source.fromFile(path).getLines.toIndexedSeq
    val rddRows: RDD[String] = sc.parallelize(lines, partitions);

    val data: RDD[Row] = rddRows.map(o => {
      val r = o.split(separator, -1)
      var typed = (Array.range(0, r.length)).map(idx => {
        val column = columns(idx)
        val value = r(idx)

        column match {
          case "integer"   => if (Try(value.toInt).isSuccess) value.toInt else null.asInstanceOf[Int]
          case "double"    => if (Try(value.toDouble).isSuccess) value.toDouble else null.asInstanceOf[Double]
          case "logical"   => if (Try(value.toBoolean).isSuccess) value.toBoolean else null.asInstanceOf[Boolean]
          case "timestamp" => if (Try(new java.sql.Timestamp(value.toLong * 1000)).isSuccess) new java.sql.Timestamp(value.toLong * 1000) else null.asInstanceOf[java.sql.Timestamp]
          case _ => value
        }
      })

      org.apache.spark.sql.Row.fromSeq(typed)
    })

    data
  }

  /**
   * Utilities for performing mutations
   */

  def addSequentialIndex(
    df: DataFrame,
    from: Int,
    id: String) : DataFrame = {
      val sqlContext = df.sqlContext
      sqlContext.createDataFrame(
        df.rdd.zipWithIndex.map {
          case (row: Row, i: Long) => Row.fromSeq(row.toSeq :+ (i.toDouble + from.toDouble))
        },
      df.schema.add(id, "double")
      )
  }


  def getLastIndex(df: DataFrame, id: String) : Double = {
    val numPartitions = df.rdd.partitions.length
    df.select(id).rdd.mapPartitionsWithIndex{
      (i, iter) => if (i != numPartitions - 1 || iter.isEmpty) {
        iter
      } else {
        Iterator
        .continually((iter.next(), iter.hasNext))
        .collect { case (value, false) => value }
        .take(1)
      }
    }.collect().last.getDouble(0)
  }

  def zipDataFrames(sc: SparkContext, df1: DataFrame, df2: DataFrame) : DataFrame = {
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      sqlContext.createDataFrame(
        df1.rdd.zip(df2.rdd).map { case (r1, r2) => Row.merge(r1, r2) },
        StructType(df1.schema ++ df2.schema))
  }

  def unboxString(x: Option[String]) = x match {
    case Some(s) => s
    case None => ""
  }

  def getAncestry(obj: AnyRef, simpleName: Boolean = true): Array[String] = {
    def supers(cl: Class[_]): List[Class[_]] = {
      if (cl == null) Nil else cl :: supers(cl.getSuperclass)
    }
  supers(obj.getClass).map(if (simpleName) _.getSimpleName else _.getName).toArray
  }
}



