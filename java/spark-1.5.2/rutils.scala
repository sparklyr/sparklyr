package sparklyr

import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.nio.charset.StandardCharsets

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

            bytes
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
}
