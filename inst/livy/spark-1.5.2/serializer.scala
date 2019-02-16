//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

class Serializer(tracker: JVMObjectTracker) {
  import java.io.{DataInputStream, DataOutputStream}
  import java.nio.charset.StandardCharsets
  import java.sql.{Date, Time, Timestamp}
  import java.util.Calendar

  import scala.collection.JavaConverters._
  import scala.collection.mutable.WrappedArray

  type ReadObject = (DataInputStream, Char) => Object
  type WriteObject = (DataOutputStream, Object) => Boolean

  var sqlSerDe: (ReadObject, WriteObject) = _

  def registerSqlSerDe(sqlSerDe: (ReadObject, WriteObject)): Unit = {
    this.sqlSerDe = sqlSerDe
  }

  def readObjectType(dis: DataInputStream): Char = {
    dis.readByte().toChar
  }

  def readObject(dis: DataInputStream): Object = {
    val dataType = readObjectType(dis)
    readTypedObject(dis, dataType)
  }

  def readTypedObject(
    dis: DataInputStream,
    dataType: Char): Object = {
      dataType match {
        case 'n' => null
        case 'i' => new java.lang.Integer(readInt(dis))
        case 'd' => new java.lang.Double(readDouble(dis))
        case 'b' => new java.lang.Boolean(readBoolean(dis))
        case 'c' => readString(dis)
        case 'e' => readMap(dis)
        case 'r' => readBytes(dis)
        case 'a' => readArray(dis)
        case 'l' => readList(dis)
        case 'D' => readDate(dis)
        case 't' => readTime(dis)
        case 'j' => tracker.getObject(readString(dis))
        case _ =>
          if (sqlSerDe == null || sqlSerDe._1 == null) {
            throw new IllegalArgumentException (s"Invalid type $dataType")
          } else {
            val obj = (sqlSerDe._1)(dis, dataType)
            if (obj == null) {
              throw new IllegalArgumentException (s"Invalid type $dataType")
            } else {
              obj
            }
          }
      }
    }

  def readBytes(in: DataInputStream): Array[Byte] = {
    val len = readInt(in)
    val out = new Array[Byte](len)
    val bytesRead = in.readFully(out)
    out
  }

  def readInt(in: DataInputStream): Int = {
    in.readInt()
  }

  def readDouble(in: DataInputStream): Double = {
    in.readDouble()
  }

  def readStringBytes(in: DataInputStream, len: Int): String = {
    val bytes = new Array[Byte](len)
    in.readFully(bytes)
    if (bytes(len - 1) != 0) throw new IllegalArgumentException("length and stream do not match")
    val str = new String(bytes.dropRight(1), StandardCharsets.UTF_8)
    str
  }

  def readString(in: DataInputStream): String = {
    val len = in.readInt()
    readStringBytes(in, len)
  }

  def readBoolean(in: DataInputStream): Boolean = {
    val intVal = in.readInt()
    if (intVal == 0) false else true
  }

  def readDate(in: DataInputStream): Date = {
    Date.valueOf(readString(in))
  }

  def readTime(in: DataInputStream): Timestamp = {
    val seconds = in.readDouble()
    val sec = Math.floor(seconds).toLong
    val t = new Timestamp(sec * 1000L)
    t.setNanos(((seconds - sec) * 1e9).toInt)
    t
  }

  def readBytesArr(in: DataInputStream): Array[Array[Byte]] = {
    val len = readInt(in)
    (0 until len).map(_ => readBytes(in)).toArray
  }

  def readIntArr(in: DataInputStream): Array[Int] = {
    val len = readInt(in)
    (0 until len).map(_ => readInt(in)).toArray
  }

  def readDoubleArr(in: DataInputStream): Array[Double] = {
    val len = readInt(in)
    (0 until len).map(_ => readDouble(in)).toArray
  }

  def readBooleanArr(in: DataInputStream): Array[Boolean] = {
    val len = readInt(in)
    (0 until len).map(_ => readBoolean(in)).toArray
  }

  def readStringArr(in: DataInputStream): Array[String] = {
    val len = readInt(in)
    (0 until len).map(_ => readString(in)).toArray
  }

  def readArrayArr(in: DataInputStream): Array[_] = {
    val len = readInt(in)
    (0 until len).map(_ => readArray(in)).toArray
  }

  def readListArr(in: DataInputStream): Array[_] = {
    val len = readInt(in)
    (0 until len).map(_ => readList(in)).toArray
  }

  def readDateArr(in: DataInputStream): Array[Date] = {
    val len = readInt(in)
    (0 until len).map(_ => readDate(in)).toArray
  }

  def readTimeArr(in: DataInputStream): Array[Timestamp] = {
    val len = readInt(in)
    (0 until len).map(_ => readTime(in)).toArray
  }

  def readArray(dis: DataInputStream): Array[_] = {
    val arrType = readObjectType(dis)
    arrType match {
      case 'i' => readIntArr(dis)
      case 'c' => readStringArr(dis)
      case 'd' => readDoubleArr(dis)
      case 'b' => readBooleanArr(dis)
      case 'j' => readStringArr(dis).map(x => tracker.getObject(x))
      case 'r' => readBytesArr(dis)
      case 'a' => readArrayArr(dis)
      case 'l' => readListArr(dis)
      case 'D' => readDateArr(dis)
      case 't' => readTimeArr(dis)
      case _ =>
        if (sqlSerDe == null || sqlSerDe._1 == null) {
          throw new IllegalArgumentException (s"Invalid array type $arrType")
        } else {
          val len = readInt(dis)
          (0 until len).map { _ =>
            val obj = (sqlSerDe._1)(dis, arrType)
            if (obj == null) {
              throw new IllegalArgumentException (s"Invalid array type $arrType")
            } else {
              obj
            }
          }.toArray
        }
    }
  }

  def readList(dis: DataInputStream): Array[Object] = {
    val len = readInt(dis)
    (0 until len).map(_ => readObject(dis)).toArray
  }

  def readMap(in: DataInputStream): java.util.Map[Object, Object] = {
    val len = readInt(in)
    if (len > 0) {
      val keys = readArray(in).asInstanceOf[Array[Object]]
      val values = readList(in)

      keys.zip(values).toMap.asJava
    } else {
      new java.util.HashMap[Object, Object]()
    }
  }

  def writeType(dos: DataOutputStream, typeStr: String): Unit = {
    typeStr match {
      case "void"      => dos.writeByte('n')
      case "character" => dos.writeByte('c')
      case "double"    => dos.writeByte('d')
      case "integer"   => dos.writeByte('i')
      case "logical"   => dos.writeByte('b')
      case "date"      => dos.writeByte('D')
      case "time"      => dos.writeByte('t')
      case "raw"       => dos.writeByte('r')
      case "array"     => dos.writeByte('a')
      case "list"      => dos.writeByte('l')
      case "map"       => dos.writeByte('e')
      case "jobj"      => dos.writeByte('j')
      case "strarray"  => dos.writeByte('f')
      case _ => throw new IllegalArgumentException(s"Invalid type $typeStr")
    }
  }

  private def writeKeyValue(dos: DataOutputStream, key: Object, value: Object): Unit = {
    if (key == null) {
      throw new IllegalArgumentException("Key in map can't be null.")
    } else if (!key.isInstanceOf[String]) {
      throw new IllegalArgumentException(s"Invalid map key type: ${key.getClass.getName}")
    }

    writeString(dos, key.asInstanceOf[String])
    writeObject(dos, value)
  }

  def writeObject(dos: DataOutputStream, obj: Object): Unit = {
    if (obj == null) {
      writeType(dos, "void")
    } else {
      val value = if (obj.isInstanceOf[WrappedArray[_]]) {
        obj.asInstanceOf[WrappedArray[_]].toArray
      } else {
        obj
      }

      value match {
        case v: java.lang.Character =>
          writeType(dos, "character")
          writeString(dos, v.toString)
        case v: java.lang.String =>
          writeType(dos, "character")
          writeString(dos, v)
        case v: java.lang.Long =>
          writeType(dos, "double")
          writeDouble(dos, v.toDouble)
        case v: java.lang.Float =>
          writeType(dos, "double")
          writeDouble(dos, v.toDouble)
        case v: java.math.BigDecimal =>
          writeType(dos, "double")
          writeDouble(dos, scala.math.BigDecimal(v).toDouble)
        case v: java.lang.Double =>
          writeType(dos, "double")
          writeDouble(dos, v)
        case v: java.lang.Byte =>
          writeType(dos, "integer")
          writeInt(dos, v.toInt)
        case v: java.lang.Short =>
          writeType(dos, "integer")
          writeInt(dos, v.toInt)
        case v: java.lang.Integer =>
          writeType(dos, "integer")
          writeInt(dos, v)
        case v: java.lang.Boolean =>
          writeType(dos, "logical")
          writeBoolean(dos, v)
        case v: java.sql.Date =>
          writeType(dos, "date")
          writeDate(dos, v)
        case v: java.sql.Time =>
          writeType(dos, "time")
          writeTime(dos, v)
        case v: java.sql.Timestamp =>
          writeType(dos, "time")
          writeTime(dos, v)
        case v: org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema =>
          writeType(dos, "list")
          writeInt(dos, v.length)
          v.toSeq.foreach(elem => writeObject(dos, elem.asInstanceOf[AnyRef]))
        case v: Array[Byte] =>
          writeType(dos, "raw")
          writeBytes(dos, v)
        case v: Array[Char] =>
          writeType(dos, "array")
          writeStringArr(dos, v.map(_.toString))
        case v: Array[Short] =>
          writeType(dos, "array")
          writeIntArr(dos, v.map(_.toInt))
        case v: Array[Int] =>
          writeType(dos, "array")
          writeIntArr(dos, v)
        case v: Array[Long] =>
          writeType(dos, "array")
          writeDoubleArr(dos, v.map(_.toDouble))
        case v: Array[Float] =>
          writeType(dos, "array")
          writeDoubleArr(dos, v.map(_.toDouble))
        case v: Array[Double] =>
          writeType(dos, "array")
          writeDoubleArr(dos, v)
        case v: Array[Boolean] =>
          writeType(dos, "array")
          writeBooleanArr(dos, v)
        case v: Array[Timestamp] =>
          writeType(dos, "array")
          writeTimestampArr(dos, v)
        case v: Array[Date] =>
          writeType(dos, "array")
          writeDateArr(dos, v)
        case v: Array[String] =>
          writeType(dos, "strarray")
          writeFastStringArr(dos, v)
        case v: Array[Object] =>
          writeType(dos, "list")
          writeInt(dos, v.length)
          v.foreach(elem => writeObject(dos, elem))
        case v: Tuple3[String, String, Any] =>
          // Tuple3
          writeType(dos, "list")
          writeInt(dos, v.productArity)
          v.productIterator.foreach(elem => writeObject(dos, elem.asInstanceOf[Object]))
        case v: java.util.Properties =>
          writeType(dos, "jobj")
          writeJObj(dos, value)
        case v: java.util.Map[_, _] =>
          writeType(dos, "map")
          writeInt(dos, v.size)
          val iter = v.entrySet.iterator
          while(iter.hasNext) {
            val entry = iter.next
            val key = entry.getKey
            val value = entry.getValue
            writeKeyValue(dos, key.asInstanceOf[Object], value.asInstanceOf[Object])
          }
        case v: scala.collection.Map[_, _] =>
          writeType(dos, "map")
          writeInt(dos, v.size)
          v.foreach { case (key, value) =>
            writeKeyValue(dos, key.asInstanceOf[Object], value.asInstanceOf[Object])
          }
        case _ =>
          if (sqlSerDe == null || sqlSerDe._2 == null || !(sqlSerDe._2)(dos, value)) {
            writeType(dos, "jobj")
            writeJObj(dos, value)
          }
      }
    }
  }

  def writeInt(out: DataOutputStream, value: Int): Unit = {
    out.writeInt(value)
  }

  def writeDouble(out: DataOutputStream, value: Double): Unit = {
    out.writeDouble(value)
  }

  def writeBoolean(out: DataOutputStream, value: Boolean): Unit = {
    val intValue = if (value) 1 else 0
    out.writeInt(intValue)
  }

  def writeDate(out: DataOutputStream, value: Date): Unit = {
    writeString(out, value.toString)
  }

  def writeTime(out: DataOutputStream, value: Time): Unit = {
    out.writeDouble(value.getTime.toDouble / 1000.0)
  }

  def writeTime(out: DataOutputStream, value: Timestamp): Unit = {
    out.writeDouble((value.getTime / 1000).toDouble + value.getNanos.toDouble / 1e9)
  }

  def writeString(out: DataOutputStream, value: String): Unit = {
    val utf8 = value.getBytes(StandardCharsets.UTF_8)
    val len = utf8.length
    out.writeInt(len)
    out.write(utf8, 0, len)
  }

  def writeBytes(out: DataOutputStream, value: Array[Byte]): Unit = {
    out.writeInt(value.length)
    out.write(value)
  }

  def writeJObj(out: DataOutputStream, value: Object): Unit = {
    val objId = tracker.put(value)
    writeString(out, objId)
  }

  def writeIntArr(out: DataOutputStream, value: Array[Int]): Unit = {
    writeType(out, "integer")
    out.writeInt(value.length)
    value.foreach(v => out.writeInt(v))
  }

  def writeDoubleArr(out: DataOutputStream, value: Array[Double]): Unit = {
    writeType(out, "double")
    out.writeInt(value.length)
    value.foreach(v => out.writeDouble(v))
  }

  def writeBooleanArr(out: DataOutputStream, value: Array[Boolean]): Unit = {
    writeType(out, "logical")
    out.writeInt(value.length)
    value.foreach(v => writeBoolean(out, v))
  }

  def writeTimestampArr(out: DataOutputStream, value: Array[java.sql.Timestamp]): Unit = {
    writeType(out, "time")
    out.writeInt(value.length)
    value.foreach(v => writeTime(out, v))
  }

  def timestampToUTC(millis: Long): Timestamp = {
    millis match {
      case 0 => new java.sql.Timestamp(0)
      case _ => new java.sql.Timestamp(
        millis +
        Calendar.getInstance.get(Calendar.ZONE_OFFSET) +
        Calendar.getInstance.get(Calendar.DST_OFFSET)
      )
    }
  }

  def writeDateArr(out: DataOutputStream, value: Array[java.sql.Date]): Unit = {
    writeType(out, "date")
    out.writeInt(value.length)

    // Dates are created in the local JVM time zone, so we need to convert to UTC here
    // See: https://docs.oracle.com/javase/7/docs/api/java/util/Date.html#getTimezoneOffset()

    value.foreach(v => writeTime(
      out,
      timestampToUTC(v.getTime())
    ))
  }

  def writeStringArr(out: DataOutputStream, value: Array[String]): Unit = {
    writeType(out, "character")
    out.writeInt(value.length)
    value.foreach(v => writeString(out, v))
  }

  def writeFastStringArr(out: DataOutputStream, value: Array[String]): Unit = {
    val all = value.mkString("\u0019")
    writeString(out, all)
  }
}
