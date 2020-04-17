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

import scala.collection.JavaConverters._
import scala.util.Try

object Utils {
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

  def collectImplBoolean(row: Row, idx: Int): Int = {
    val el = row(idx)
    if (el.isInstanceOf[Boolean]) if (el.asInstanceOf[Boolean]) 1 else 0 else scala.Int.MinValue
  }

  def collectImplBooleanArr(row: Row, idx: Int): Array[Int] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e => {
      if (e.isInstanceOf[Boolean]) if (el.asInstanceOf[Boolean]) 1 else 0 else scala.Int.MinValue
    }).toArray
  }

  def collectImplInteger(row: Row, idx: Int): Int = {
    val el = row(idx)
    if (el.isInstanceOf[Int]) el.asInstanceOf[Int] else scala.Int.MinValue
  }

  def collectImplIntegerArr(row: Row, idx: Int): Array[Int] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      if (e.isInstanceOf[Int]) e.asInstanceOf[Int] else scala.Int.MinValue
    ).toArray
  }

  def collectImplDouble(row: Row, idx: Int): Double = {
    val el = row(idx)
    if (el.isInstanceOf[Double]) el.asInstanceOf[Double] else scala.Double.NaN
  }

  def collectImplDoubleArr(row: Row, idx: Int): Array[Double] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      if (e.isInstanceOf[Double]) e.asInstanceOf[Double] else scala.Double.NaN
    ).toArray
  }

  def collectImplFloat(row: Row, idx: Int): Double  = {
    val el = row(idx)
    if (el.isInstanceOf[Float]) el.asInstanceOf[Float].toDouble else scala.Double.NaN
  }

  def collectImplFloatArr(row: Row, idx: Int): Array[Double] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      if (e.isInstanceOf[Float]) e.asInstanceOf[Float].toDouble else scala.Double.NaN
    ).toArray
  }

  def collectImplByte(row: Row, idx: Int): Int = {
    val el = row(idx)
    if (el.isInstanceOf[Byte]) el.asInstanceOf[Byte].toInt else scala.Int.MinValue
  }

  def collectImplByteArr(row: Row, idx: Int): Array[Int] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      if (e.isInstanceOf[Byte]) e.asInstanceOf[Byte].toInt else scala.Int.MinValue
    ).toArray
  }

  def collectImplShort(row: Row, idx: Int): Int = {
    val el = row(idx)
    if (el.isInstanceOf[Short]) el.asInstanceOf[Short].toInt else scala.Int.MinValue
  }

  def collectImplShortArr(row: Row, idx: Int): Array[Int] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      if (e.isInstanceOf[Short]) e.asInstanceOf[Short].toInt else scala.Int.MinValue
    ).toArray
  }

  def collectImplLong(row: Row, idx: Int): Double = {
    val el = row(idx)
    if (el.isInstanceOf[Long]) el.asInstanceOf[Long].toDouble else scala.Double.NaN
  }

  def collectImplLongArr(row: Row, idx: Int): Array[Double] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      if (e.isInstanceOf[Long]) e.asInstanceOf[Long].toDouble else scala.Double.NaN
    ).toArray
  }

  val collectImplForceString = (row: Row, idx: Int) => {
    val el = row(idx)
    if (el != null) el.toString() else "<NA>"
  }

  def collectImplForceStringArr(row: Row, idx: Int): Array[String] = {
    val arr = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]

    arr.map{e => {
      if (e != null) e.toString() else "<NA>"
    }}.toArray
  }

  val collectImplString = (row: Row, idx: Int) => {
    val el = row(idx)
    if (el.isInstanceOf[String]) el.asInstanceOf[String] else "<NA>"
  }

  def collectImplStringArr(row: Row, idx: Int): Array[String] = {
    val arr = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]

    arr.map{e => {
      if (e.isInstanceOf[String]) e.asInstanceOf[String] else "<NA>"
    }}.toArray
  }

  def collectImplDecimal(row: Row, idx: Int): Double = {
    val el = row(idx)
    if (el.isInstanceOf[java.math.BigDecimal])
      el.asInstanceOf[java.math.BigDecimal].doubleValue
    else
      scala.Double.NaN
  }

  def collectImplDecimalArr(row: Row, idx: Int): Array[Double] = {
    val arr = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]

    arr.map(el => {
      if (el.isInstanceOf[java.math.BigDecimal])
        el.asInstanceOf[java.math.BigDecimal].doubleValue
      else
        scala.Double.NaN
    }).toArray
  }

  def collectImplVector(row: Row, idx: Int): Array[_] = {
    val el = row(idx)
    el match {
      case null => Array.empty
      case _: Seq[_] => el.asInstanceOf[Seq[Any]].toArray
      case _ => el.getClass.getDeclaredMethod("toArray").invoke(el).asInstanceOf[Array[_]]
    }
  }

  def collectImplJSON(row: Row, idx: Int) = {
    val el = row(idx)
    el match {
      case _: String => new StructTypeAsJSON(el.asInstanceOf[String])
      case _ => collectImplDefault(row, idx)
    }
  }

  def collectImplTimestamp(row: Row, idx: Int): java.sql.Timestamp = {
    Try(row.getAs[java.sql.Timestamp](idx)).getOrElse(null)
  }

  def collectImplTimestampArr(row: Row, idx: Int): Array[java.sql.Timestamp] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      Try(e.asInstanceOf[java.sql.Timestamp]).getOrElse(null)
    ).toArray
  }

  def collectImplDate(row: Row, idx: Int): java.sql.Date = {
    Try(row.getAs[java.sql.Date](idx)).getOrElse(null)
  }

  def collectImplDateArr(row: Row, idx: Int): Array[java.sql.Date] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      Try(e.asInstanceOf[java.sql.Date]).getOrElse(null)
    ).toArray
  }

  def collectImplDefault(row: Row, idx: Int) = {
    row(idx)
  }

  def collectColumns(
    iter: Iterator[Row],
    dtypes: Array[(String, String)],
    separator: String,
    maxNumRows: Int
  ): Array[_] = {
    class ColumnCtx[T <: Any : Manifest](collector: (Row, Int) => T, numRows: Int) {
      val col = new Array[T](numRows)
      def collect(row: Row, colIdx: Int, rowIdx: Int): Unit = {
        col(rowIdx) = collector(row, colIdx)
      }
      def column(): Array[T] = {
        col
      }
      def collector(): (Row, Int) => T = collector
    }

    def mkColumnCtx(colType: String, numRows: Int) = {
      val ReDecimalType = "(DecimalType.*)".r
      val ReVectorType  = "(.*VectorUDT.*)".r

      def newColumnCtx[T <: Any : Manifest](collector: (Row, Int) => T) = {
        new ColumnCtx[T](collector, numRows)
      }

      colType match {
        case "BooleanType"          => newColumnCtx[Int](collectImplBoolean)
        case "IntegerType"          => newColumnCtx[Int](collectImplInteger)
        case "DoubleType"           => newColumnCtx[Double](collectImplDouble)
        case "StringType"           => newColumnCtx[String](collectImplString)
        case "LongType"             => newColumnCtx[Double](collectImplLong)

        case "ByteType"             => newColumnCtx[Int](collectImplByte)
        case "FloatType"            => newColumnCtx[Double](collectImplFloat)
        case "ShortType"            => newColumnCtx[Int](collectImplShort)
        case "Decimal"              => newColumnCtx[String](collectImplForceString)

        case "TimestampType"        => newColumnCtx[java.sql.Timestamp](collectImplTimestamp)
        case "CalendarIntervalType" => newColumnCtx[String](collectImplForceString)
        case "DateType"             => newColumnCtx[java.sql.Date](collectImplDate)

        case ReDecimalType(_)       => newColumnCtx[Double](collectImplDecimal)
        case ReVectorType(_)        => newColumnCtx[Any](collectImplVector)
        case StructTypeAsJSON.DType => newColumnCtx[Any](collectImplJSON)

        case "ArrayType(BooleanType,true)"           => newColumnCtx[Array[Int]](collectImplBooleanArr)
        case "ArrayType(IntegerType,true)"           => newColumnCtx[Array[Int]](collectImplIntegerArr)
        case "ArrayType(DoubleType,true)"            => newColumnCtx[Array[Double]](collectImplDoubleArr)
        case "ArrayType(StringType,true)"            => newColumnCtx[Array[String]](collectImplStringArr)
        case "ArrayType(LongType,true)"              => newColumnCtx[Array[Double]](collectImplLongArr)
        case "ArrayType(ByteType,true)"              => newColumnCtx[Array[Int]](collectImplByteArr)
        case "ArrayType(FloatType,true)"             => newColumnCtx[Array[Double]](collectImplFloatArr)
        case "ArrayType(ShortType,true)"             => newColumnCtx[Array[Int]](collectImplShortArr)
        case "ArrayType(DecimalType,true)"           => newColumnCtx[Array[Double]](collectImplDecimalArr)
        case "ArrayType(TimestampType,true)"         => newColumnCtx[Array[java.sql.Timestamp]](collectImplTimestampArr)
        case "ArrayType(CalendarIntervalType,true)"  => newColumnCtx[Array[String]](collectImplForceStringArr)
        case "ArrayType(DateType,true)"              => newColumnCtx[Array[java.sql.Date]](collectImplDateArr)

        case "ArrayType(BooleanType,false)"          => newColumnCtx[Array[Int]](collectImplBooleanArr)
        case "ArrayType(IntegerType,false)"          => newColumnCtx[Array[Int]](collectImplIntegerArr)
        case "ArrayType(DoubleType,false)"           => newColumnCtx[Array[Double]](collectImplDoubleArr)
        case "ArrayType(StringType,false)"           => newColumnCtx[Array[String]](collectImplStringArr)
        case "ArrayType(LongType,false)"             => newColumnCtx[Array[Double]](collectImplLongArr)
        case "ArrayType(ByteType,false)"             => newColumnCtx[Array[Int]](collectImplByteArr)
        case "ArrayType(FloatType,false)"            => newColumnCtx[Array[Double]](collectImplFloatArr)
        case "ArrayType(ShortType,false)"            => newColumnCtx[Array[Int]](collectImplShortArr)
        case "ArrayType(DecimalType,false)"          => newColumnCtx[Array[Double]](collectImplDecimalArr)
        case "ArrayType(TimestampType,false)"        => newColumnCtx[Array[java.sql.Timestamp]](collectImplTimestampArr)
        case "ArrayType(CalendarIntervalType,false)" => newColumnCtx[Array[String]](collectImplForceStringArr)
        case "ArrayType(DateType,false)"             => newColumnCtx[Array[java.sql.Date]](collectImplDateArr)

        case "NullType"             => newColumnCtx[String](collectImplForceString)

        case _                      => newColumnCtx[Any](collectImplDefault)
      }
    }

    val columnCtx = dtypes.map{dtype => mkColumnCtx(dtype._2, maxNumRows)}
    var numRows: Int = 0
    while (iter.hasNext && numRows < maxNumRows) {
      val row: Row = iter.next
      (0 until dtypes.length).foreach(colIdx => {
        columnCtx(colIdx).collect(row, colIdx, numRows)
      })
      numRows += 1
    }
    // merge any string column into a single string delimited by `separator`
    val StringCollectors = Array[(Row, Int) => String](collectImplString, collectImplForceString)
    val res: Array[Any] = columnCtx.map(x => {
      val column = x.column.take(numRows)
      if (StringCollectors contains x.collector) {
        val str = column.mkString(separator)
        if (str.isEmpty) str else str + separator
      } else {
        column
      }
    }).toArray

    res
  }

  def collect(df: DataFrame, separator: String, optimizeMemUtil: Boolean): Array[_] = {
    val (transformed_df, dtypes) = DFCollectionUtils.prepareDataFrameForCollection(df)
    val collectColumnsFromIterator = collectColumns(_: Iterator[Row], dtypes, separator, df.count.toInt)
    if (optimizeMemUtil) {
      // attempt to not fetch all partitions all at once
      val rdd = transformed_df.rdd.cache
      collectColumnsFromIterator(rdd.toLocalIterator)
    } else {
      collectColumnsFromIterator(transformed_df.collect.iterator)
    }
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
          case "integer"  => if (Try(value.toInt).isSuccess) value.toInt else null
          case "double"  => if (Try(value.toDouble).isSuccess) value.toDouble else null
          case "logical" => if (Try(value.toBoolean).isSuccess) value.toBoolean else null
          case "timestamp" => if (Try(new java.sql.Timestamp(value.toLong * 1000)).isSuccess) new java.sql.Timestamp(value.toLong * 1000) else null
          case "date" => if (Try(new java.sql.Date(value.toLong * 86400000)).isSuccess) new java.sql.Date(value.toLong * 86400000) else null
          case _ => if (value == "NA") null else value
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
          case "integer"   => if (Try(value.toInt).isSuccess) value.toInt else null
          case "double"    => if (Try(value.toDouble).isSuccess) value.toDouble else null
          case "logical"   => if (Try(value.toBoolean).isSuccess) value.toBoolean else null
          case "timestamp" => if (Try(new java.sql.Timestamp(value.toLong * 1000)).isSuccess) new java.sql.Timestamp(value.toLong * 1000) else null
          case "date" => if (Try(new java.sql.Date(value.toLong * 86400000)).isSuccess) new java.sql.Date(value.toLong * 86400000) else null
          case _ => if (value == "NA") null else value
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

  def portIsAvailable(port: Int, inetAddress: InetAddress) = {
    var ss: ServerSocket = null
    var available = false

    Try {
        ss = new ServerSocket(port, 1, inetAddress)
        available = true
    }

    if (ss != null) {
        Try {
            ss.close();
        }
    }

    available
  }

  def nextPort(port: Int, inetAddress: InetAddress) = {
    var freePort = port + 1
    while (!portIsAvailable(freePort, inetAddress) && freePort - port < 100)
      freePort += 1

    // give up after 100 port searches
    if (freePort - port < 100) freePort else 0;
  }

  def buildStructTypeForIntegerField(): StructType = {
    val fields = Array(StructField("id", IntegerType, false))
    StructType(fields)
  }

  def buildStructTypeForLongField(): StructType = {
    val fields = Array(StructField("id", LongType, false))
    StructType(fields)
  }

  def mapRddLongToRddRow(rdd: RDD[Long]): RDD[Row] = {
    rdd.map(x => org.apache.spark.sql.Row(x))
  }

  def mapRddIntegerToRddRow(rdd: RDD[Long]): RDD[Row] = {
    rdd.map(x => org.apache.spark.sql.Row(x.toInt))
  }

  def readWholeFiles(sc: SparkContext, inputPath: String): RDD[Row] = {
    sc.wholeTextFiles(inputPath).map {
      l => Row(l._1, l._2)
    }
  }

  def unionRdd(context: org.apache.spark.SparkContext, rdds: Seq[org.apache.spark.rdd.RDD[org.apache.spark.sql.Row]]):
    org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = {
    context.union(rdds)
  }

  def collectIter(iter: Iterator[Row], size: Int, df: DataFrame, separator: String, optimizeMemUtil: Boolean): Array[_] = {
    val dtypes = df.dtypes
    collectColumns(iter, dtypes, separator, size)
  }
}
