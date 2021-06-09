package sparklyr

import org.apache.spark.sql._

import scala.Option
import scala.reflect.ClassTag
import scala.util.Try

object Collectors {
  val ReDecimalType = "(^DecimalType(\\(.*\\)?)$)".r
  val ReVectorType  = "(.*VectorUDT.*)".r
  val ReBooleanArrayType = "(^ArrayType\\(BooleanType,(true|false)\\)$)".r
  val ReByteArrayType = "(^ArrayType\\(ByteType,(true|false)\\)$)".r
  val ReShortArrayType = "(^ArrayType\\(ShortType,(true|false)\\)$)".r
  val ReIntegerArrayType = "(^ArrayType\\(IntegerType,(true|false)\\)$)".r
  val ReLongArrayType = "(^ArrayType\\(LongType,(true|false)\\)$)".r
  val ReDecimalArrayType = "(^ArrayType\\(DecimalType(\\(.*\\)?),(true|false)\\)$)".r
  val ReFloatArrayType = "(^ArrayType\\(FloatType,(true|false)\\)$)".r
  val ReDoubleArrayType = "(^ArrayType\\(DoubleType,(true|false)\\)$)".r
  val ReStringArrayType = "(^ArrayType\\(StringType,(true|false)\\)$)".r
  val ReTimestampArrayType = "(^ArrayType\\(TimestampType,(true|false)\\)$)".r
  val ReDateArrayType = "(^ArrayType\\(DateType,(true|false)\\)$)".r

  def collectLogical(row: Row, idx: Int): Logical = {
    extractLogical(row(idx))
  }

  def collectLogicalArr(row: Row, idx: Int): Array[Logical] = {
    val arr = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]

    if (arr == null) {
      null
    } else {
      arr.map(extractLogical).toArray
    }
  }

  private[this] def extractLogical(x: Any): Logical = {
    x match {
      case b: Boolean => Logical(b.compare(false))
      case _ => Logical(scala.Int.MinValue)
    }
  }

  def collectIntegralType[T: scala.math.Integral](
    row: Row,
    idx: Int
  )(implicit t: ClassTag[T]): Int = {
    extractIntegralType[T](row(idx))
  }

  def collectIntegralTypeArr[T: scala.math.Integral](
    row: Row,
    idx: Int
  )(implicit t: ClassTag[T]): Array[Int] = {
    val arr = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]

    if (arr == null) {
      null
    } else {
      arr.map(extractIntegralType[T]).toArray
    }
  }

  def collectByteArr(row: Row, idx: Int): Raw = {
    val intValues = collectIntegralTypeArr[Byte](row, idx)
    if (intValues != null) {
      Raw(intValues.map(x => x.byteValue).toArray)
    } else {
      Raw(null)
    }
  }

  private[this] def extractIntegralType[T: scala.math.Integral](
    x: Any
  )(implicit t: ClassTag[T]): Int = {
    x match {
      case i: T => implicitly[scala.math.Integral[T]].toInt(i)
      case _=> scala.Int.MinValue
    }
  }

  def collectNumericType[T: scala.math.Numeric](
    row: Row,
    idx: Int
  )(implicit t: ClassTag[T]): Numeric = {
    extractNumericType[T](row(idx))
  }

  def collectNumericTypeArr[T: scala.math.Numeric](
    row: Row,
    idx: Int
  )(implicit t: ClassTag[T]): Array[Numeric] = {
    val arr = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]

    if (arr == null) {
      null
    } else {
      arr.map(extractNumericType[T]).toArray
    }
  }

  private[this] def extractNumericType[T: scala.math.Numeric](
    x: Any
  )(implicit t: ClassTag[T]): Numeric = {
    Numeric(
      if (x == null) {
        None
      } else {
        x match {
          case v: T => Some(implicitly[scala.math.Numeric[T]].toDouble(v))
          case _ => Some(scala.Double.NaN)
        }
      }
    )
  }

  def collectBigDecimal(row: Row, idx: Int): Numeric = {
    extractBigDecimal(row(idx))
  }

  def collectBigDecimalArr(row: Row, idx: Int): Array[Numeric] = {
    val arr = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]

    if (arr == null) {
      null
    } else {
      arr.map(extractBigDecimal).toArray
    }
  }

  private[this] def extractBigDecimal(x: Any): Numeric = {
    Numeric(
      if (x == null) {
        None
      } else {
        x match {
          case d: java.math.BigDecimal => Some(d.doubleValue)
          case _ => Some(scala.Double.NaN)
        }
      }
    )
  }

  val collectForceString = (row: Row, idx: Int) => {
    extractStringRepr(row(idx))
  }

  def collectForceStringArr(row: Row, idx: Int): Array[String] = {
    val arr = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]

    if (arr == null) {
      null
    } else {
      arr.map(extractStringRepr).toArray
    }
  }

  private[this] def extractStringRepr(x: Any): String = {
    if (x == null) {
      "<NA>"
    } else {
      x.toString
    }
  }

  val collectString: (Row, Int) => String = (row: Row, idx: Int) => {
    extractString(row(idx))
  }

  def collectStringArr(row: Row, idx: Int): Array[String] = {
    val arr = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]

    if (arr == null) {
      null
    } else {
      arr.map(extractString).toArray
    }
  }

  private[this] def extractString(x: Any): String = {
    x match {
      case s: String => s
      case _ => "<NA>"
    }
  }

  def collectVector(row: Row, idx: Int): Array[_] = {
    val el = row(idx)
    el match {
      case null => Array.empty
      case _: Seq[_] => el.asInstanceOf[Seq[Any]].toArray
      case _ => el.getClass.getDeclaredMethod("toArray").invoke(el).asInstanceOf[Array[_]]
    }
  }

  def collectJSON(row: Row, idx: Int) = {
    val el = row(idx)

    el match {
      case _: String => new StructTypeAsJSON(el.asInstanceOf[String])
      case _ => collectDefault(row, idx)
    }
  }

  def collectTimestamp(row: Row, idx: Int): java.sql.Timestamp = {
    extractTimestamp(row(idx))
  }

  def collectTimestampArr(row: Row, idx: Int): Array[java.sql.Timestamp] = {
    val arr = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]

    if (arr == null) {
      null
    } else {
      arr.map(extractTimestamp).toArray
    }
  }

  private[this] def extractTimestamp(x: Any): java.sql.Timestamp = {
    x match {
      case t: java.sql.Timestamp => t
      case _ => null
    }
  }

  def collectDate(row: Row, idx: Int): DaysSinceEpoch = {
    extractDaysSinceEpoch(row(idx))
  }

  def extractDaysSinceEpoch(x: Any): DaysSinceEpoch = {
    DaysSinceEpoch(
      x match {
        case dt: java.sql.Date =>
          Some(org.apache.spark.sql.catalyst.util.DateTimeUtils.fromJavaDate(dt))
        case _ => None
      }
    )
  }

  def collectDateArr(row: Row, idx: Int): Array[DaysSinceEpoch] = {
    val arr = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]

    if (arr == null) {
      null
    } else {
      arr.map(extractDaysSinceEpoch).toArray
    }
  }

  def collectDefault(row: Row, idx: Int) = {
    row(idx)
  }

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

    def newColumnCtx[T <: Any : Manifest](collector: (Row, Int) => T) = {
      new ColumnCtx[T](collector, numRows)
    }

    colType match {
      case "BooleanType"          => newColumnCtx[Logical](Collectors.collectLogical)
      case "ByteType"             => newColumnCtx[Int](Collectors.collectIntegralType[Byte])
      case "ShortType"            => newColumnCtx[Int](Collectors.collectIntegralType[Short])
      case "IntegerType"          => newColumnCtx[Int](Collectors.collectIntegralType[Int])
      case "LongType"             => newColumnCtx[Numeric](Collectors.collectNumericType[Long])
      case "Decimal"              => newColumnCtx[String](Collectors.collectForceString)
      case ReDecimalType(_*)      => newColumnCtx[Numeric](Collectors.collectBigDecimal)
      case "FloatType"            => newColumnCtx[Numeric](Collectors.collectNumericType[Float])
      case "DoubleType"           => newColumnCtx[Numeric](Collectors.collectNumericType[Double])
      case "StringType"           => newColumnCtx[String](Collectors.collectString)
      case "TimestampType"        => newColumnCtx[java.sql.Timestamp](Collectors.collectTimestamp)
      case "CalendarIntervalType" => newColumnCtx[String](Collectors.collectForceString)
      case "DateType"             => newColumnCtx[DaysSinceEpoch](Collectors.collectDate)
      case ReVectorType(_*)       => newColumnCtx[Any](Collectors.collectVector)
      case StructTypeAsJSON.DType => newColumnCtx[Any](Collectors.collectJSON)

      case ReBooleanArrayType(_*)   => newColumnCtx[Array[Logical]](Collectors.collectLogicalArr)
      case ReByteArrayType(_*)      => newColumnCtx[Raw](Collectors.collectByteArr)
      case ReShortArrayType(_*)     => newColumnCtx[Array[Int]](Collectors.collectIntegralTypeArr[Short])
      case ReIntegerArrayType(_*)   => newColumnCtx[Array[Int]](Collectors.collectIntegralTypeArr[Int])
      case ReLongArrayType(_*)      => newColumnCtx[Array[Numeric]](Collectors.collectNumericTypeArr[Long])
      case ReDecimalArrayType(_*)   => newColumnCtx[Array[Numeric]](Collectors.collectBigDecimalArr)
      case ReFloatArrayType(_*)     => newColumnCtx[Array[Numeric]](Collectors.collectNumericTypeArr[Float])
      case ReDoubleArrayType(_*)    => newColumnCtx[Array[Numeric]](Collectors.collectNumericTypeArr[Double])
      case ReStringArrayType(_*)    => newColumnCtx[Array[String]](Collectors.collectStringArr)
      case ReTimestampArrayType(_*) => newColumnCtx[Array[java.sql.Timestamp]](Collectors.collectTimestampArr)
      case ReDateArrayType(_*)      => newColumnCtx[Array[DaysSinceEpoch]](Collectors.collectDateArr)
      case "ArrayType(CalendarIntervalType,true)"  => newColumnCtx[Array[String]](Collectors.collectForceStringArr)

      case "ArrayType(CalendarIntervalType,false)" => newColumnCtx[Array[String]](Collectors.collectForceStringArr)

      case "NullType"             => newColumnCtx[String](Collectors.collectForceString)

      case _                      => newColumnCtx[Any](Collectors.collectDefault)
    }
  }
}
