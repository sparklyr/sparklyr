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

object Utils {
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

  def parallelize(
    sc: SparkContext,
    num_rows: Int,
    serialized_cols: Array[Array[Byte]],
    timestamp_col_idxes: Seq[Int],
    string_col_idxes: Seq[Int],
    partitions: Int
  ): RDD[InternalRow] = {
    if (serialized_cols.isEmpty) {
      throw new IllegalArgumentException("Serialized columns byte array is empty.")
    }
    val cols = serialized_cols.map(
      serialized_col => {
        val bis = new ByteArrayInputStream(serialized_col)
        val dis = new DataInputStream(bis)

        RUtils.validateSerializationFormat(dis)
        RUtils.unserializeColumn(dis)
      }
    ).toArray
    val num_cols = cols.length
    for (c <- timestamp_col_idxes) {
      cols(c) = cols(c).asInstanceOf[Array[_]].map(
        x => x.asInstanceOf[Double].longValue * 1000000
      ).toArray
    }
    for (c <- string_col_idxes) {
      cols(c) = cols(c).asInstanceOf[Array[_]].map(
        x => UTF8String.fromString(x.asInstanceOf[String])
      ).toArray
    }
    val rows = (0 until num_rows).par.map(r => {
      val variableLengthBytes = (0 until num_cols).map(c =>
        cols(c)(r) match {
          case x: UTF8String => x.numBytes
          case x: RawSXP => x.buf.length
          case _ => 0
        }
      ).sum
      var variableLengthOffset = UnsafeRow.calculateBitSetWidthInBytes(cols.length) + 8 * num_cols
      val row = UnsafeRow.createFromByteArray(variableLengthOffset + variableLengthBytes, num_cols)
      (0 until num_cols).map(
        c => {
          if (cols(c)(r) == null) {
            row.setNullAt(c)
          } else {
            cols(c)(r) match {
              case x: Boolean => row.setBoolean(c, x)
              case x: Integer => row.setInt(c, x)
              case x: Double => row.setDouble(c, x)
              case x: Long => row.setLong(c, x)
              case x: UTF8String => {
                variableLengthOffset = writeVariableLengthField(
                  variableLengthOffset,
                  row,
                  c,
                  x.getBytes
                )
              }
              case x: RawSXP => {
                variableLengthOffset = writeVariableLengthField(
                  variableLengthOffset,
                  row,
                  c,
                  x.buf
                )
              }
              case _ => throw new IllegalArgumentException("Unsupported column type")
            }
          }
        }
      )

      row
    }).toArray

    val x: RDD[InternalRow] = sc.parallelize(rows, partitions)

    x
  }

  private[this] def writeVariableLengthField(
    variableLengthOffset: Int,
    row: UnsafeRow,
    ordinal: Int,
    buf: Array[Byte]
  ) : Int = {
    row.setLong(ordinal, (variableLengthOffset.toLong << 32) | buf.length.toLong)
    Platform.copyMemory(
      buf,
      Platform.BYTE_ARRAY_OFFSET,
      row.getBaseObject,
      row.getBaseOffset + variableLengthOffset,
      buf.length
    )

    variableLengthOffset + buf.length
  }

  def createDataFrame(
    sc: SparkContext,
    rows: Array[_],
    partitions: Int
  ): RDD[Row] = {
    var data = rows.map(o => {
      val r = o.asInstanceOf[Array[_]]
      org.apache.spark.sql.Row.fromSeq(r)
    })

    sc.parallelize(data, partitions)
  }

  def classExists(name: String): Boolean = {
    scala.util.Try(Class.forName(name)).isSuccess
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

  def extractValue(row: Row, colIdx: Int) = {
    val dtype = row.schema.fields(colIdx).dataType

    dtype match {
      case _: BooleanType => row.getBoolean(colIdx)
      case _: ByteType => row.getByte(colIdx)
      case _: ShortType => row.getShort(colIdx)
      case _: IntegerType => row.getInt(colIdx)
      case _: LongType => row.getLong(colIdx)
      case _: FloatType => row.getFloat(colIdx)
      case _: DoubleType => row.getDouble(colIdx)
      case _: DecimalType => row.getDecimal(colIdx)
      case _: StringType => row.getString(colIdx)
      case _: BinaryType => row.getAs[Array[Byte]](colIdx)
      case _: TimestampType => row.getTimestamp(colIdx)
      case _: DateType => row.getDate(colIdx)
      case _: ArrayType => row.getAs[scala.collection.Seq[Any]](colIdx)
      case _: MapType => row.getAs[scala.collection.Map[Any, Any]](colIdx)
      case _: StructType => row.getAs[Row](colIdx)
      case _ => row.getString(colIdx)
    }
  }

  def as(dtype: DataType) = {
    (value: Any) => {
      dtype match {
        case _: ByteType => value.asInstanceOf[Byte]
        case _: ShortType => value.asInstanceOf[Short]
        case _: IntegerType => value.asInstanceOf[Int]
        case _: LongType => value.asInstanceOf[Long]
        case _: FloatType => value.asInstanceOf[Float]
        case _: DoubleType => value.asInstanceOf[Double]
        case _: DecimalType => value.asInstanceOf[java.math.BigDecimal]
        case _: StringType => value.asInstanceOf[String]
        case _: BinaryType => value.asInstanceOf[Array[Byte]]
        case _: TimestampType => value.asInstanceOf[java.sql.Timestamp]
        case _: DateType => value.asInstanceOf[java.sql.Date]
        case _: ArrayType => value.asInstanceOf[scala.collection.Seq[Any]]
        case _: MapType => value.asInstanceOf[scala.collection.Map[Any, Any]]
        case _: StructType => value.asInstanceOf[Row]
        case _ => value.asInstanceOf[String]
      }
    }
  }

  def asDouble(x: Any): Double = {
    x match {
      case v: Byte => v.doubleValue
      case v: Short => v.doubleValue
      case v: Int => v.toDouble
      case v: Long => v.toDouble
      case v: Double => v
      case v: Float => v.toDouble
      case v: java.math.BigDecimal => v.doubleValue
      case v: java.sql.Timestamp => v.getTime.toDouble
      case v: java.sql.Date => v.getTime.toDouble
      case v: java.util.Date => v.getTime.toDouble
      case _ => Double.NaN
    }
  }

  def asLong(x: Any): Long = {
    x match {
      case v: Byte => v.toLong
      case v: Short => v.toLong
      case v: Int => v.toLong
      case v: Long => v
      case v: Float => v.toLong
      case v: Double => v.toLong
      case v: java.math.BigDecimal => v.longValue
      case v: java.sql.Timestamp => v.getTime
      case v: java.sql.Date => v.getTime
      case v: java.util.Date => v.getTime
      case _ => throw new IllegalArgumentException("unsupported input type")
    }
  }

  def setProperties(
    keys: Seq[String],
    values: Seq[String]
  ): java.util.Properties = {
    setProperties(new java.util.Properties, keys, values)
  }

  def setProperties(
    x: java.util.Properties,
    keys: Seq[String],
    values: Seq[String]
  ): java.util.Properties = {
    for (i <- (0 until keys.length)) {
      x.setProperty(keys(i), values(i))
    }

    x
  }
}
