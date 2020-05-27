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

  private[this] def toArray(columns: Iterable[Collectors.ColumnCtx[_]], numRows: Int, separator: String): Array[_] = {
    // merge any string column into a single string delimited by `separator`
    val StringCollectors = Array[(Row, Int) => String](Collectors.collectString, Collectors.collectForceString)
    val res: Array[Any] = columns.map(x => {
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

  private[this] def collectRows(
    iter: Iterator[Row],
    dtypes: Array[(String, String)],
    separator: String,
    maxNumRows: Int
  ): Array[_] = {
    val columns = dtypes.map{dtype => Collectors.mkColumnCtx(dtype._2, maxNumRows)}

    var numRows: Int = 0
    while (iter.hasNext && numRows < maxNumRows) {
      val row: Row = iter.next
      (0 until dtypes.length).foreach(colIdx => {
        columns(colIdx).collect(row, colIdx, numRows)
      })
      numRows += 1
    }

    toArray(columns, numRows, separator)
  }

  private[this] def collectColumns(local: Array[Row], dtypes: Array[(String, String)], separator: String): Array[_] = {
    val numRows: Int = local.length
    val columns = dtypes.map{dtype => Collectors.mkColumnCtx(dtype._2, numRows)}

    (0 until dtypes.length).foreach(colIdx => {
      (0 until numRows).foreach(rowIdx => {
        columns(colIdx).collect(local(rowIdx), colIdx, rowIdx)
      })
    })

    toArray(columns, numRows, separator)
  }

  def collect(df: DataFrame, separator: String, impl: String): Array[_] = {
    val (transformed_df, dtypes) = DFCollectionUtils.prepareDataFrameForCollection(df)
    val collectRowsFromIterator = collectRows(_: Iterator[Row], dtypes, separator, df.count.toInt)
    impl match {
      case "row-wise" => collectRowsFromIterator(transformed_df.collect.iterator)
      case "row-wise-iter" => {
        // attempt to not fetch all partitions all at once
        val rdd = transformed_df.rdd.cache
        collectRowsFromIterator(rdd.toLocalIterator)
      }
      case "column-wise" => collectColumns(df.collect, dtypes, separator)
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

  def createDataFrameFromColumnArr(
    sc: SparkContext,
    columnArr: Array[_],
    columns: Array[String],
    partitions: Int
  ): RDD[Row] = {
    if (columnArr.isEmpty) {
      sc.emptyRDD
    } else {
      val numCols: Int = columnArr.size
      val numRows: Int = columnArr(0).asInstanceOf[Array[_]].size
      val data = (0 until numRows).map(rowIdx => {
        org.apache.spark.sql.Row.fromSeq(
          (0 until numCols).map(colIdx => {
            val column = columns(colIdx)
            val value = columnArr(colIdx).asInstanceOf[Array[_]](rowIdx)

            column match {
              case "timestamp" => Try(new java.sql.Timestamp(value.asInstanceOf[Long] * 1000)).getOrElse(null)
              case "date" => Try(new java.sql.Date(value.asInstanceOf[Long] * 86400000)).getOrElse(null)
              case _ => if (value == "NA") null else value
            }
          })
        )
      })
      sc.parallelize(data, partitions)
    }
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

  def collectIter(iter: Iterator[Row], dtypes: Array[Any], size: Int, separator: String): Array[_] = {
    collectRows(iter, dtypes.map(x => x.asInstanceOf[(String, String)]).toArray, separator, size)
  }
}
