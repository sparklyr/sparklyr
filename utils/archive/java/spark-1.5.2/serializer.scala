package sparklyr

import java.io.{DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.sql.{Date, Time, Timestamp}
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.TimeZone

import scala.collection.JavaConverters._
import scala.collection.convert.Wrappers.SeqWrapper
import scala.collection.mutable.WrappedArray
import scala.Option

object Serializer {
  private[this] val kMsPerDay: Long = 24 * 60 * 60 * 1000

  def readObjectType(dis: DataInputStream): Char = {
    dis.readByte().toChar
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
    intVal != 0
  }

  def readDate(in: DataInputStream): Date = {
    val n = readInt(in)
    if (n == Integer.MIN_VALUE) {
      null
    } else {
      new Date(java.lang.Long.valueOf(n) * kMsPerDay)
    }
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

  def readDateArr(in: DataInputStream): Array[Date] = {
    val len = readInt(in)
    (0 until len).map(_ => readDate(in)).toArray
  }

  def readTimeArr(in: DataInputStream): Array[Timestamp] = {
    val len = readInt(in)
    (0 until len).map(_ => readTime(in)).toArray
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
      case "strarray"  => dos.writeByte('f')
      case "list"      => dos.writeByte('l')
      case "map"       => dos.writeByte('e')
      case "jobj"      => dos.writeByte('j')
      case "json"      => dos.writeByte('J')
      case "sparkapplybinaryresult" => dos.writeByte('$')
      case _ => throw new IllegalArgumentException(s"Invalid type $typeStr")
    }
  }

  def writeInt(out: DataOutputStream, value: Int): Unit = {
    out.writeInt(value)
  }

  def writeDouble(out: DataOutputStream, value: Double): Unit = {
    out.writeDouble(value)
  }

  def writeNumeric(out: DataOutputStream, x: Numeric): Unit = {
    if (x.value.isEmpty) {
      out.writeInt(0x7FF00000)
      out.writeInt(1954)
    } else {
      writeDouble(out, x.value.get)
    }
  }

  def writeBoolean(out: DataOutputStream, value: Boolean): Unit = {
    out.writeInt(value.compare(false))
  }

  def writeLogical(out: DataOutputStream, value: Logical): Unit = {
    out.writeInt(value.intValue)
  }

  def writeDate(out: DataOutputStream, value: Date): Unit = {
    writeInt(
      out,
      if (null == value)
        Integer.MIN_VALUE
      else {
        (value.getTime / kMsPerDay).toInt
      }
    )
  }

  def writeDate(out: DataOutputStream, d: DaysSinceEpoch): Unit = {
    writeInt(out, d.value.getOrElse(Integer.MIN_VALUE))
  }

  def timestampToSeconds(value: java.util.Date): Double = {
    if (null == value) {
      Double.NaN
    } else {
      val seconds = value.getTime.toDouble / 1e3

      value match {
        case ts: Timestamp =>
          seconds + ts.getNanos.toDouble / 1e9
        case _ =>
          seconds
      }
    }
  }

  def writeTime(out: DataOutputStream, value: java.util.Date): Unit = {
    out.writeDouble(timestampToSeconds(value))
  }

  def writeString(out: DataOutputStream, value: String): Unit = {
    val utf8 = value.getBytes(StandardCharsets.UTF_8)
    val len = utf8.length
    out.writeInt(len)
    out.write(utf8, 0, len)
  }

  def writeBytes(out: DataOutputStream, value: Array[Byte]): Unit = {
    if (value == null) {
      out.writeInt(-1)
    } else {
      out.writeInt(value.length)
      out.write(value)
    }
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

  def writeNumericArr(out: DataOutputStream, value: Array[Numeric]): Unit = {
    writeType(out, "double")
    out.writeInt(value.length)
    value.foreach(v => Serializer.writeNumeric(out, v))
  }

  def writeBooleanArr(out: DataOutputStream, value: Array[Boolean]): Unit = {
    writeType(out, "logical")
    out.writeInt(value.length)
    value.foreach(v => writeBoolean(out, v))
  }

  def writeLogicalArr(out: DataOutputStream, value: Array[Logical]): Unit = {
    writeType(out, "logical")
    out.writeInt(value.length)
    value.foreach(v => writeInt(out, v.intValue))
  }

  def writeTimestampArr(out: DataOutputStream, value: Array[java.sql.Timestamp]): Unit = {
    writeType(out, "time")
    out.writeInt(value.length)
    value.foreach(v => writeTime(out, v))
  }

  def writeStringArr(out: DataOutputStream, value: Array[String]): Unit = {
    val all = value.mkString("\u0019")
    writeString(out, all)
  }

  def writeDateArr(out: DataOutputStream, value: Array[java.sql.Date]): Unit = {
    writeType(out, "date")
    out.writeInt(value.length)

    value.foreach(v => writeDate(out, v))
  }

  def writeDateArr(out: DataOutputStream, value: Array[DaysSinceEpoch]): Unit = {
    writeType(out, "date")
    out.writeInt(value.length)

    value.foreach(v => writeDate(out, v))
  }
}

class Serializer(tracker: JVMObjectTracker) {
  type ReadObject = (DataInputStream, Char) => Object
  type WriteObject = (DataOutputStream, Object) => Boolean

  var sqlSerDe: (ReadObject, WriteObject) = _

  def registerSqlSerDe(sqlSerDe: (ReadObject, WriteObject)): Unit = {
    this.sqlSerDe = sqlSerDe
  }

  def readObject(dis: DataInputStream): Object = {
    val dataType = Serializer.readObjectType(dis)
    readTypedObject(dis, dataType)
  }

  def readList(dis: DataInputStream): Array[Object] = {
    val len = Serializer.readInt(dis)
    (0 until len).map(_ => readObject(dis)).toArray
  }

  def readTypedObject(
    dis: DataInputStream,
    dataType: Char): Object = {
      dataType match {
        case 'n' => null
        case 'i' => new java.lang.Integer(Serializer.readInt(dis))
        case 'd' => new java.lang.Double(Serializer.readDouble(dis))
        case 'b' => new java.lang.Boolean(Serializer.readBoolean(dis))
        case 'c' => Serializer.readString(dis)
        case 'e' => readMap(dis)
        case 'r' => Serializer.readBytes(dis)
        case 'a' => readArray(dis)
        case 'l' => readList(dis)
        case 'D' => Serializer.readDate(dis)
        case 't' => Serializer.readTime(dis)
        case 'j' => tracker.getObject(Serializer.readString(dis))
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

  def readArray(dis: DataInputStream): Array[_] = {
    val arrType = Serializer.readObjectType(dis)
    arrType match {
      case 'i' => Serializer.readIntArr(dis)
      case 'c' => Serializer.readStringArr(dis)
      case 'd' => Serializer.readDoubleArr(dis)
      case 'b' => Serializer.readBooleanArr(dis)
      case 'j' => Serializer.readStringArr(dis).map(x => tracker.getObject(x))
      case 'r' => Serializer.readBytesArr(dis)
      case 'a' => readArrayArr(dis)
      case 'l' => readListArr(dis)
      case 'D' => Serializer.readDateArr(dis)
      case 't' => Serializer.readTimeArr(dis)
      case _ =>
        if (sqlSerDe == null || sqlSerDe._1 == null) {
          throw new IllegalArgumentException (s"Invalid array type $arrType")
        } else {
          val len = Serializer.readInt(dis)
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

  def readListArr(in: DataInputStream): Array[_] = {
    val len = Serializer.readInt(in)
    (0 until len).map(_ => readList(in)).toArray
  }

  def readMap(in: DataInputStream): Map[_, _] = {
    val len = Serializer.readInt(in)
    if (len > 0) {
      val keys = readArray(in)
      val values = readList(in)

      keys.zip(values).toMap
    } else {
      Map()
    }
  }

  def readArrayArr(in: DataInputStream): Array[_] = {
    val len = Serializer.readInt(in)
    (0 until len).map(_ => readArray(in)).toArray
  }

  private def writeKeyValue(dos: DataOutputStream, key: Object, value: Object): Unit = {
    if (key == null) {
      throw new IllegalArgumentException("Key in map can't be null.")
    } else if (!key.isInstanceOf[String]) {
      throw new IllegalArgumentException(s"Invalid map key type: ${key.getClass.getName}")
    }

    Serializer.writeString(dos, key.asInstanceOf[String])
    writeObject(dos, value)
  }

  def writeObject(dos: DataOutputStream, obj: Object): Unit = {
    if (obj == null) {
      Serializer.writeType(dos, "void")
    } else {
      val value = (
        if (obj.isInstanceOf[WrappedArray[_]]) {
          obj.asInstanceOf[WrappedArray[_]].toArray
        } else {
          obj match {
            case s @ SeqWrapper(_) => s.toArray
            case _ => obj
          }
        }
      )

      if (value.isInstanceOf[Array[java.lang.Float]]) {
        Serializer.writeType(dos, "array")
        Serializer.writeDoubleArr(dos, value.asInstanceOf[Array[java.lang.Float]].map(_.toDouble))
      } else {
        value match {
          case v: java.lang.Character =>
            Serializer.writeType(dos, "character")
            Serializer.writeString(dos, v.toString)
          case v: java.lang.String =>
            Serializer.writeType(dos, "character")
            Serializer.writeString(dos, v)
          case v: java.lang.Long =>
            Serializer.writeType(dos, "double")
            Serializer.writeDouble(dos, v.toDouble)
          case v: java.lang.Float =>
            Serializer.writeType(dos, "double")
            Serializer.writeDouble(dos, v.toDouble)
          case v: java.math.BigDecimal =>
            Serializer.writeType(dos, "double")
            Serializer.writeDouble(dos, scala.math.BigDecimal(v).toDouble)
          case v: java.lang.Double =>
            Serializer.writeType(dos, "double")
            Serializer.writeDouble(dos, v)
          case v: Numeric =>
            Serializer.writeType(dos, "double")
            Serializer.writeNumeric(dos, v)
          case v: java.lang.Byte =>
            Serializer.writeType(dos, "integer")
            Serializer.writeInt(dos, v.toInt)
          case v: java.lang.Short =>
            Serializer.writeType(dos, "integer")
            Serializer.writeInt(dos, v.toInt)
          case v: java.lang.Integer =>
            Serializer.writeType(dos, "integer")
            Serializer.writeInt(dos, v)
          case v: java.lang.Boolean =>
            Serializer.writeType(dos, "logical")
            Serializer.writeBoolean(dos, v)
          case v: Logical =>
            Serializer.writeType(dos, "logical")
            Serializer.writeLogical(dos, v)
          case v: java.sql.Date =>
            Serializer.writeType(dos, "date")
            Serializer.writeDate(dos, v)
          case v: DaysSinceEpoch =>
            Serializer.writeType(dos, "date")
            Serializer.writeDate(dos, v)
          case v: java.sql.Timestamp =>
            Serializer.writeType(dos, "time")
            Serializer.writeTime(dos, v)
          case v: java.sql.Time =>
            Serializer.writeType(dos, "time")
            Serializer.writeTime(dos, v)
          case v: StructTypeAsJSON =>
            Serializer.writeType(dos, "json")
            Serializer.writeString(dos, v.json)
          case v: org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema =>
            Serializer.writeType(dos, "list")
            Serializer.writeInt(dos, v.length)
            v.toSeq.foreach(elem => writeObject(dos, elem.asInstanceOf[AnyRef]))
          case v: Array[Byte] =>
            Serializer.writeType(dos, "raw")
            Serializer.writeBytes(dos, v)
          case v: Raw =>
            Serializer.writeType(dos, "raw")
            Serializer.writeBytes(dos, v.value)
          case v: Array[Char] =>
            Serializer.writeType(dos, "strarray")
            Serializer.writeStringArr(dos, v.map(_.toString))
          case v: Array[Short] =>
            Serializer.writeType(dos, "array")
            Serializer.writeIntArr(dos, v.map(_.toInt))
          case v: Array[Int] =>
            Serializer.writeType(dos, "array")
            Serializer.writeIntArr(dos, v)
          case v: Array[Long] =>
            Serializer.writeType(dos, "array")
            Serializer.writeDoubleArr(dos, v.map(_.toDouble))
          case v: Array[Float] =>
            Serializer.writeType(dos, "array")
            Serializer.writeDoubleArr(dos, v.map(_.toDouble))
          case v: Array[Double] =>
            Serializer.writeType(dos, "array")
            Serializer.writeDoubleArr(dos, v)
          case v: Array[Numeric] =>
            Serializer.writeType(dos, "array")
            Serializer.writeNumericArr(dos, v)
          case v: Array[Boolean] =>
            Serializer.writeType(dos, "array")
            Serializer.writeBooleanArr(dos, v)
          case v: Array[Logical] =>
            Serializer.writeType(dos, "array")
            Serializer.writeLogicalArr(dos, v)
          case v: Array[Timestamp] =>
            Serializer.writeType(dos, "array")
            Serializer.writeTimestampArr(dos, v)
          case v: Array[Date] =>
            Serializer.writeType(dos, "array")
            Serializer.writeDateArr(dos, v)
          case v: Array[DaysSinceEpoch] =>
            Serializer.writeType(dos, "array")
            Serializer.writeDateArr(dos, v)
          case v: Array[String] =>
            Serializer.writeType(dos, "strarray")
            Serializer.writeStringArr(dos, v)
          case v: Array[Object] =>
            Serializer.writeType(dos, "list")
            Serializer.writeInt(dos, v.length)
            v.foreach(elem => writeObject(dos, elem))
          case v @ Tuple3(_: String, _: String, _: Any) =>
            // Tuple3
            Serializer.writeType(dos, "list")
            Serializer.writeInt(dos, v.productArity)
            v.productIterator.foreach(elem => writeObject(dos, elem.asInstanceOf[Object]))
          case v: java.util.Properties =>
            Serializer.writeType(dos, "jobj")
            writeJObj(dos, value)
          case v: java.util.Map[_, _] =>
            Serializer.writeType(dos, "map")
            Serializer.writeInt(dos, v.size)
            val iter = v.entrySet.iterator
            while(iter.hasNext) {
              val entry = iter.next
              val key = entry.getKey
              val value = entry.getValue
              writeKeyValue(dos, key.asInstanceOf[Object], value.asInstanceOf[Object])
            }
          case v: scala.collection.Map[_, _] =>
            Serializer.writeType(dos, "map")
            Serializer.writeInt(dos, v.size)
            v.foreach { case (key, value) =>
              writeKeyValue(dos, key.asInstanceOf[Object], value.asInstanceOf[Object])
            }
          case _ =>
            if (sqlSerDe == null || sqlSerDe._2 == null || !(sqlSerDe._2)(dos, value)) {
              Serializer.writeType(dos, "jobj")
              writeJObj(dos, value)
            }
        }
      }
    }
  }

  def writeJObj(out: DataOutputStream, value: Object): Unit = {
    val objId = tracker.put(value)
    Serializer.writeString(out, objId)
  }
}
