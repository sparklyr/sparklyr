package sparklyr

import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.collection.Iterator

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Column
import org.apache.spark.sql.Row

case class RawSXP(val buf: Array[Byte])

object RUtils {
  private[this] val XDR_FORMAT = 'X'
  private[this] val CHARSXP = 9
  private[this] val LGLSXP = 10
  private[this] val INTSXP = 13
  private[this] val REALSXP = 14
  private[this] val STRSXP = 16
  private[this] val VECSXP = 19
  private[this] val RAWSXP = 24
  private[this] val NILVALUE_SXP = 254
  private[this] val NA_INTEGER = Integer.MIN_VALUE

  def validateSerializationFormat(
    dis: DataInputStream,
    verbose: Boolean = false
  ): Unit = {
    val format = Serializer.readObjectType(dis)
    dis.readByte()  // skip the 2nd byte which we currently do not use
    if (XDR_FORMAT != format)
      throw new IllegalArgumentException(
        s"Unsupported R serialization format '$format'"
      )

    val version = Serializer.readInt(dis)
    val writerVersion = Serializer.readInt(dis)
    val minReaderVersion = Serializer.readInt(dis)

    if (version != 2)
      throw new IllegalArgumentException(
        s"Unsupported R serialization version '$version'"
      )

    if (verbose) {
      val logger = new Logger("Utils", 0)
      logger.log(s"R serialization version: $version")
      logger.log(s"R serialization writer version: $writerVersion")
      logger.log(s"R serialization min reader version: $minReaderVersion")
    }
  }

  def unserializeColumn(dis: DataInputStream) : Array[Any] = {
    val logger = new Logger("RUtils", 0)

    val dtype = readDataType(dis) // read column type
    if (VECSXP != dtype)
      logger.logWarning(s"Unexpected column data type $dtype")

    val num_rows = readLength(dis)

    (0L until num_rows).map(
      r => {
        val elem_dtype = readDataType(dis)

        if (NILVALUE_SXP == elem_dtype) {
          null
        } else {
          val num_elems = readLength(dis)

          if (RAWSXP == elem_dtype) {
            val bytes = new Array[Byte](num_elems.intValue)
            dis.readFully(bytes)

            RawSXP(bytes)
          } else {
            if (1 != num_elems)
              throw new IllegalArgumentException(
                s"Unexpected number of elements: $num_elems"
              )

            elem_dtype match {
              case LGLSXP => {
                val v = Serializer.readInt(dis)
                if (NA_INTEGER == v) null else v != 0
              }
              case INTSXP => {
                val v = Serializer.readInt(dis)
                if (NA_INTEGER == v) null else v
              }
              case REALSXP => {
                val bytes = new Array[Byte](8)
                dis.readFully(bytes)
                val hw: Long = extractInt(bytes, 0) & 0x00000000FFFFFFFFL
                val lw: Long = extractInt(bytes, 4) & 0x00000000FFFFFFFFL
                if (hw == 0x7FF00000L && lw == 1954L)
                  null
                else
                  java.lang.Double.longBitsToDouble((hw << 32) | lw)
              }
              case STRSXP => {
                val ctype = readDataType(dis)
                if (CHARSXP != ctype)
                  logger.logWarning(
                    s"Unexpected character type $ctype found in string expression"
                  )

                /* these are currently limited to 2^31 -1 bytes */
                val strlen = Serializer.readInt(dis)
                if (-1L == strlen) {
                  null
                } else {
                  val bytes = new Array[Byte](strlen.intValue)
                  dis.readFully(bytes)

                  new String(bytes, StandardCharsets.UTF_8)
                }
              }
              case _ =>
                throw new IllegalArgumentException(s"Invalid type $elem_dtype")
            }
          }
        }
      }.asInstanceOf[Any]
    ).toArray
  }

  private[this] def unserializeIntColumn(
    dis: DataInputStream
  ) : Array[Int] = {
    Array.fill[Int](5)(0)
  }

  private[this] def unserializeStringColumn(
    dis: DataInputStream
  ) : Array[String] = {
    Array.fill[String](5)("")
  }

  private[this] def readDataType(
    dis: DataInputStream
  ): Int = {
    Serializer.readInt(dis) & 0xFF
  }

  private[this] def readLength(
    dis: DataInputStream
  ): Long = {
    val x = Serializer.readInt(dis)
    val len = (
      if (-1 == x) {
        // TODO: if necessary, find a reasonably simple way to support
        // array with more than INT_MAX elements in Scala

        // val upper: Long = Serializer.readInt(dis)
        // val lower: Long = Serializer.readInt(dis)
        // (upper << 32) + lower
        throw new IllegalArgumentException(
          "Vector with length greater than INT_MAX (i.e., 'LONG_VECTOR') is not supported yet"
        )
      } else {
        x
      }
    )

    if (len < 0)
      throw new IllegalArgumentException(
        s"Negative serialized vector length: $len"
      )

    len
  }

  private[this] def extractInt(bytes: Array[Byte], offset: Int): Int = {
    (0 until 4).map(
      i => (bytes(offset + i).intValue & 0xFF) << (24 - i * 8)
    ).reduce(
      (p, q) => p | q
    )
  }

  private[this] def rVersion(v: Int, p: Int, s: Int): Int = {
    v * 65536 + p * 256 + s
  }

  def writeXdrHeader(dos: DataOutputStream): Unit = {
    // serialization format
    dos.writeByte(XDR_FORMAT)
    // 2nd byte should be '\n' per convention
    dos.writeByte('\n')
    // serialization format version
    dos.writeInt(2)
    // R version (bogus)
    dos.writeInt(rVersion(1, 4, 0))
    // Min reader version
    dos.writeInt(rVersion(1, 4, 0))
  }

  def writeDataFrameHeader(
    dos: DataOutputStream,
    dtypes: Seq[(String, String)]
  ): Unit = {
    // NOTE: strictly speaking this just writes the following:
    // List of length 5
    //   |_ List of column name(s)
    //   |_ List of 1-based timestamp column index(s)
    //   |_ List of 1-based date column index(s)
    //   |_ List of 1-based struct column index(s)
    //   |_ List of column(s), where each column is a vector of element(s)
    writeFlags(dos, dtype = VECSXP)
    writeLength(dos, 5)

    writeStringValues(dos, dtypes.map(x => x._1))

    val timestampColIdxes = ArrayBuffer[Int]()
    val dateColIdxes = ArrayBuffer[Int]()
    val structColIdxes = ArrayBuffer[Int]()
    (1 to dtypes.length).map(
      idx => {
        val colType = dtypes(idx - 1)._2

        colType match {
          case "TimestampType"                     => timestampColIdxes += idx
          case "DateType"                          => dateColIdxes += idx
          case Collectors.ReTimestampArrayType(_*) => timestampColIdxes += idx
          case Collectors.ReDateArrayType(_*)      => dateColIdxes += idx
          case StructTypeAsJSON.DType              => structColIdxes += idx
          case _                                   =>
        }
      }
    )
    writeIntValues(dos, timestampColIdxes.toSeq)
    writeIntValues(dos, dateColIdxes.toSeq)
    writeIntValues(dos, structColIdxes.toSeq)

    writeFlags(dos, dtype = VECSXP)
    writeLength(dos, dtypes.length)
  }

  def writeColumn(
    dos: DataOutputStream,
    numRows: Int,
    colIter: Iterator[Row],
    dtype: String
  ): Unit = {
    val writer = (
      dtype match {
        case "BooleanType" => booleanColumnWriter()
        case "ByteType" => integralColumnWriter[Byte]()
        case "ShortType" => integralColumnWriter[Short]()
        case "IntegerType" => integralColumnWriter[Int]()
        case "FloatType" => numericColumnWriter[Float]()
        case "LongType" => numericColumnWriter[Long]()
        case "DoubleType" => numericColumnWriter[Double]()
        case Collectors.ReDecimalType(_*) => decimalColumnWriter()
        case "StringType" => stringColumnWriter()
        case "TimestampType" => timestampColumnWriter()
        case "DateType" => dateColumnWriter()
        case Collectors.ReBooleanArrayType(_*) =>
          arrayColumnWriter(LGLSXP, writeLglValue)
        case Collectors.ReByteArrayType(_*) => rawColumnWriter()
        case Collectors.ReShortArrayType(_*) =>
          arrayColumnWriter(INTSXP, writeIntegralValue[Short])
        case Collectors.ReIntegerArrayType(_*) =>
          arrayColumnWriter(INTSXP, writeIntegralValue[Int])
        case Collectors.ReLongArrayType(_*) =>
          arrayColumnWriter(REALSXP, writeNumericValue[Long])
        case Collectors.ReDecimalArrayType(_*) =>
          arrayColumnWriter(REALSXP, writeDecimalValue)
        case Collectors.ReFloatArrayType(_*) =>
          arrayColumnWriter(REALSXP, writeNumericValue[Float])
        case Collectors.ReDoubleArrayType(_*) =>
          arrayColumnWriter(REALSXP, writeNumericValue[Double])
        case Collectors.ReStringArrayType(_*) =>
          arrayColumnWriter(STRSXP, writeStringValue)
        case Collectors.ReTimestampArrayType(_*) =>
          arrayColumnWriter(REALSXP, writeTimestampValue)
        case Collectors.ReDateArrayType(_*) =>
          arrayColumnWriter(INTSXP, writeDateValue)
        case StructTypeAsJSON.DType => stringColumnWriter()
        case _ => {
          throw new IllegalArgumentException(
            s"Serializing Spark dataframe column of type '$dtype' to RDS is unsupported"
          )
        }
      }
    )

    writer(dos, numRows, colIter)
  }

  private[this] def writeDateValue(dos: DataOutputStream, v: Any): Unit = {
    Serializer.writeDate(
      dos,
      Collectors.extractDaysSinceEpoch(v)
    )
  }

  private[this] def booleanColumnWriter(): (DataOutputStream, Int, Iterator[Row]) => Unit = {
    (dos: DataOutputStream, numRows: Int, colIter: Iterator[Row]) => {
      writeFlags(dos, dtype = LGLSXP)
      writeLength(dos, numRows)
      colIter.foreach(row => writeLglValue(dos, row.get(0)))
    }
  }

  private[this] def writeLglValue(dos: DataOutputStream, v: Any): Unit = {
    dos.writeInt({
      if (null == v) {
        NA_INTEGER
      } else {
        val b = v.asInstanceOf[Boolean]
        b.compare(false)
      }}
    )
  }

  private[this] def rawColumnWriter(): (DataOutputStream, Int, Iterator[Row]) => Unit = {
    (dos: DataOutputStream, numRows: Int, colIter: Iterator[Row]) => {
      writeFlags(dos, dtype = VECSXP)
      writeLength(dos, numRows)
      colIter.foreach(row => writeRawValue(dos, row.get(0)))
    }
  }

  private[this] def integralColumnWriter[SrcType : scala.math.Integral](): (DataOutputStream, Int, Iterator[Row]) => Unit = {
    (dos: DataOutputStream, numRows: Int, colIter: Iterator[Row]) => {
      writeFlags(dos, dtype = INTSXP)
      writeLength(dos, numRows)
      colIter.foreach(row => writeIntegralValue(dos, row.get(0)))
    }
  }

  private[this] def writeIntValues(dos: DataOutputStream, vals: Seq[Int]): Unit = {
    writeFlags(dos, dtype = INTSXP)
    writeLength(dos, vals.length)
    vals.map(x => writeIntegralValue[Int](dos, x))
  }

  private[this] def writeIntegralValue[SrcType : scala.math.Integral](
    dos: DataOutputStream,
    v: Any
  ): Unit = {
    dos.writeInt({
      if (null == v) {
        NA_INTEGER
      } else {
        implicitly[scala.math.Integral[SrcType]].toInt(
          v.asInstanceOf[SrcType]
        )
      }
    })
  }

  private[this] def writeRawValue(dos: DataOutputStream, v: Any): Unit = {
    writeFlags(dos, dtype = RAWSXP)
    if (null == v) {
      writeLength(dos, -1)
    } else {
      val buf = v.asInstanceOf[scala.collection.mutable.WrappedArray[java.lang.Byte]].array
      writeLength(dos, buf.length)
      val byteArray = new Array[Byte](buf.length)
      for (i <- buf.indices) {
        byteArray(i) = buf(i).asInstanceOf[Byte]
      }
      dos.write(byteArray)

    }
  }

  private[this] def numericColumnWriter[SrcType : scala.math.Numeric](): (DataOutputStream, Int, Iterator[Row]) => Unit = {
    (dos: DataOutputStream, numRows: Int, colIter: Iterator[Row]) => {
      writeFlags(dos, dtype = REALSXP)
      writeLength(dos, numRows)
      colIter.foreach(row => writeNumericValue[SrcType](dos, row.get(0)))
    }
  }

  private[this] def writeNumericValue[SrcType : scala.math.Numeric](
    dos: DataOutputStream,
    v: Any
  ): Unit = {
    Serializer.writeNumeric(
      dos,
      Numeric(
        if (null == v) {
          None
        } else {
          Some(
            implicitly[scala.math.Numeric[SrcType]].toDouble(
              v.asInstanceOf[SrcType]
            )
          )
        }
      )
    )
  }

  private[this] def decimalColumnWriter(): (DataOutputStream, Int, Iterator[Row]) => Unit = {
    (dos: DataOutputStream, numRows: Int, colIter: Iterator[Row]) => {
      writeFlags(dos, dtype = REALSXP)
      writeLength(dos, numRows)
      colIter.foreach(row => writeDecimalValue(dos, row.get(0)))
    }
  }

  private[this] def writeDecimalValue(dos: DataOutputStream, v: Any): Unit = {
    Serializer.writeNumeric(
      dos,
      Numeric(
        v match {
          case d: BigDecimal => Some(d.doubleValue)
          case _ => None
        }
      )
    )
  }

  private[this] def stringColumnWriter(): (DataOutputStream, Int, Iterator[Row]) => Unit = {
    (dos: DataOutputStream, numRows: Int, colIter: Iterator[Row]) => {
      writeFlags(dos, dtype = STRSXP)
      writeLength(dos, numRows)
      colIter.foreach(row => writeStringValue(dos, row.get(0)))
    }
  }

  private[this] def timestampColumnWriter(): (DataOutputStream, Int, Iterator[Row]) => Unit = {
    (dos: DataOutputStream, numRows: Int, colIter: Iterator[Row]) => {
      writeFlags(dos, dtype = REALSXP)
      writeLength(dos, numRows)
      colIter.foreach(row => writeTimestampValue(dos, row.get(0)))
    }
  }

  private[this] def writeTimestampValue(dos: DataOutputStream, v: Any): Unit = {
    dos.writeDouble(
      v match {
        case d: java.util.Date => Serializer.timestampToSeconds(d)
        case _ => Double.NaN
      }
    )
  }

  private[this] def dateColumnWriter(): (DataOutputStream, Int, Iterator[Row]) => Unit = {
    (dos: DataOutputStream, numRows: Int, colIter: Iterator[Row]) => {
      writeFlags(dos, dtype = INTSXP)
      writeLength(dos, numRows)
      colIter.foreach(row => writeDateValue(dos, row.get(0)))
    }
  }

  private[this] def writeStringValues(
    dos: DataOutputStream,
    vals: Seq[String]
  ): Unit = {
    writeFlags(dos, dtype = STRSXP)
    writeLength(dos, vals.length)
    vals.map(x => writeStringValue(dos, x))
  }

  private[this] def writeStringValue(dos: DataOutputStream, v: Any): Unit = {
    writeFlags(dos, dtype = CHARSXP)

    v match {
      case s: String => Serializer.writeString(dos, s)
      case _ => writeLength(dos, -1)
    }
  }

  private[this] def arrayColumnWriter(
    elemType: Int,
    elemWriter: (DataOutputStream, Any) => Unit
  ): (DataOutputStream, Int, Iterator[Row]) => Unit = {
    (dos: DataOutputStream, numRows: Int, colIter: Iterator[Row]) => {
      writeFlags(dos, dtype = VECSXP)
      writeLength(dos, numRows)
      colIter.foreach(
        row => {
          val v = row.get(0)
          if (null == v) {
            writeFlags(dos, dtype = NILVALUE_SXP)
          } else {
            val arr = v.asInstanceOf[scala.collection.mutable.WrappedArray[_]]
            writeFlags(dos, dtype = elemType)
            writeLength(dos, arr.length)
            arr.map(b => elemWriter(dos, b))
          }
        }
      )
    }
  }

  private[this] def writeFlags(
    dos: DataOutputStream,
    dtype: Int,
    levels: Int = 0,
    isObj: Boolean = false,
    hasAttr: Boolean = false,
    hasTag: Boolean = false
  ): Unit = {
    dos.writeInt(
      (dtype & 0xFF) |
      (isObj.compare(false) << 8) |
      (hasAttr.compare(false) << 9) |
      (hasTag.compare(false) << 10) |
      (levels << 12)
    )
  }

  private[this] def writeLength(dos: DataOutputStream, length: Int): Unit = {
    // For now we only support length up to INT_MAX
    dos.writeInt(length)
  }
}
