package sparklyr

import org.apache.spark.sql._

import scala.Option
import scala.util.Try

case class Numeric(value: Option[Double])

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

  def collectBoolean(row: Row, idx: Int): Int = {
    val el = row(idx)
    if (el.isInstanceOf[Boolean]) if (el.asInstanceOf[Boolean]) 1 else 0 else scala.Int.MinValue
  }

  def collectBooleanArr(row: Row, idx: Int): Array[Int] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e => {
      if (e.isInstanceOf[Boolean]) if (el.asInstanceOf[Boolean]) 1 else 0 else scala.Int.MinValue
    }).toArray
  }

  def collectInteger(row: Row, idx: Int): Int = {
    val el = row(idx)
    if (el.isInstanceOf[Int]) el.asInstanceOf[Int] else scala.Int.MinValue
  }

  def collectIntegerArr(row: Row, idx: Int): Array[Int] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      if (e.isInstanceOf[Int]) e.asInstanceOf[Int] else scala.Int.MinValue
    ).toArray
  }

  def collectNumeric(row: Row, idx: Int): Numeric = {
    val el = row(idx)
    new Numeric(
      if (null == el) {
        None
      } else if (el.isInstanceOf[Double]) {
        Some(el.asInstanceOf[Double])
      } else {
        Some(scala.Double.NaN)
      }
    )
  }

  def collectNumericArr(row: Row, idx: Int): Array[Numeric] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      new Numeric(
        if (null == e) {
          None
        } else if (e.isInstanceOf[Double])
          Some(e.asInstanceOf[Double])
        else
          Some(scala.Double.NaN)
      )
    ).toArray
  }

  def collectFloat(row: Row, idx: Int): Double  = {
    val el = row(idx)
    if (el.isInstanceOf[Float]) el.asInstanceOf[Float].toDouble else scala.Double.NaN
  }

  def collectFloatArr(row: Row, idx: Int): Array[Double] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      if (e.isInstanceOf[Float]) e.asInstanceOf[Float].toDouble else scala.Double.NaN
    ).toArray
  }

  def collectByte(row: Row, idx: Int): Int = {
    val el = row(idx)
    if (el.isInstanceOf[Byte]) el.asInstanceOf[Byte].toInt else scala.Int.MinValue
  }

  def collectByteArr(row: Row, idx: Int): Array[Int] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      if (e.isInstanceOf[Byte]) e.asInstanceOf[Byte].toInt else scala.Int.MinValue
    ).toArray
  }

  def collectShort(row: Row, idx: Int): Int = {
    val el = row(idx)
    if (el.isInstanceOf[Short]) el.asInstanceOf[Short].toInt else scala.Int.MinValue
  }

  def collectShortArr(row: Row, idx: Int): Array[Int] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      if (e.isInstanceOf[Short]) e.asInstanceOf[Short].toInt else scala.Int.MinValue
    ).toArray
  }

  def collectLong(row: Row, idx: Int): Double = {
    val el = row(idx)
    if (el.isInstanceOf[Long]) el.asInstanceOf[Long].toDouble else scala.Double.NaN
  }

  def collectLongArr(row: Row, idx: Int): Array[Double] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      if (e.isInstanceOf[Long]) e.asInstanceOf[Long].toDouble else scala.Double.NaN
    ).toArray
  }

  val collectForceString = (row: Row, idx: Int) => {
    val el = row(idx)
    if (el != null) el.toString() else "<NA>"
  }

  def collectForceStringArr(row: Row, idx: Int): Array[String] = {
    val arr = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]

    arr.map{e => {
      if (e != null) e.toString() else "<NA>"
    }}.toArray
  }

  val collectString = (row: Row, idx: Int) => {
    val el = row(idx)
    if (el.isInstanceOf[String]) el.asInstanceOf[String] else "<NA>"
  }

  def collectStringArr(row: Row, idx: Int): Array[String] = {
    val arr = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]

    arr.map{e => {
      if (e.isInstanceOf[String]) e.asInstanceOf[String] else "<NA>"
    }}.toArray
  }

  def collectDecimal(row: Row, idx: Int): Double = {
    val el = row(idx)
    if (el.isInstanceOf[java.math.BigDecimal])
      el.asInstanceOf[java.math.BigDecimal].doubleValue
    else
      scala.Double.NaN
  }

  def collectDecimalArr(row: Row, idx: Int): Array[Double] = {
    val arr = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]

    arr.map(el => {
      if (el.isInstanceOf[java.math.BigDecimal])
        el.asInstanceOf[java.math.BigDecimal].doubleValue
      else
        scala.Double.NaN
    }).toArray
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
    Try(row.getAs[java.sql.Timestamp](idx)).getOrElse(null)
  }

  def collectTimestampArr(row: Row, idx: Int): Array[java.sql.Timestamp] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      Try(e.asInstanceOf[java.sql.Timestamp]).getOrElse(null)
    ).toArray
  }

  def collectDate(row: Row, idx: Int): java.sql.Date = {
    Try(row.getAs[java.sql.Date](idx)).getOrElse(null)
  }

  def collectDateArr(row: Row, idx: Int): Array[java.sql.Date] = {
    val el = row(idx).asInstanceOf[scala.collection.mutable.WrappedArray[_]]
    el.map(e =>
      Try(e.asInstanceOf[java.sql.Date]).getOrElse(null)
    ).toArray
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
      case "BooleanType"          => newColumnCtx[Int](Collectors.collectBoolean)
      case "ByteType"             => newColumnCtx[Int](Collectors.collectByte)
      case "ShortType"            => newColumnCtx[Int](Collectors.collectShort)
      case "IntegerType"          => newColumnCtx[Int](Collectors.collectInteger)
      case "LongType"             => newColumnCtx[Double](Collectors.collectLong)
      case "Decimal"              => newColumnCtx[String](Collectors.collectForceString)
      case ReDecimalType(_*)      => newColumnCtx[Double](Collectors.collectDecimal)
      case "FloatType"            => newColumnCtx[Double](Collectors.collectFloat)
      case "DoubleType"           => newColumnCtx[Numeric](Collectors.collectNumeric)
      case "StringType"           => newColumnCtx[String](Collectors.collectString)
      case "TimestampType"        => newColumnCtx[java.sql.Timestamp](Collectors.collectTimestamp)
      case "CalendarIntervalType" => newColumnCtx[String](Collectors.collectForceString)
      case "DateType"             => newColumnCtx[java.sql.Date](Collectors.collectDate)
      case ReVectorType(_*)       => newColumnCtx[Any](Collectors.collectVector)
      case StructTypeAsJSON.DType => newColumnCtx[Any](Collectors.collectJSON)

      case ReBooleanArrayType(_*)   => newColumnCtx[Array[Int]](Collectors.collectBooleanArr)
      case ReByteArrayType(_*)      => newColumnCtx[Array[Int]](Collectors.collectByteArr)
      case ReShortArrayType(_*)     => newColumnCtx[Array[Int]](Collectors.collectShortArr)
      case ReIntegerArrayType(_*)   => newColumnCtx[Array[Int]](Collectors.collectIntegerArr)
      case ReLongArrayType(_*)      => newColumnCtx[Array[Double]](Collectors.collectLongArr)
      case ReDecimalArrayType(_*)   => newColumnCtx[Array[Double]](Collectors.collectDecimalArr)
      case ReFloatArrayType(_*)     => newColumnCtx[Array[Double]](Collectors.collectFloatArr)
      case ReDoubleArrayType(_*)    => newColumnCtx[Array[Numeric]](Collectors.collectNumericArr)
      case ReStringArrayType(_*)    => newColumnCtx[Array[String]](Collectors.collectStringArr)
      case ReTimestampArrayType(_*) => newColumnCtx[Array[java.sql.Timestamp]](Collectors.collectTimestampArr)
      case ReDateArrayType(_*)      => newColumnCtx[Array[java.sql.Date]](Collectors.collectDateArr)
      case "ArrayType(CalendarIntervalType,true)"  => newColumnCtx[Array[String]](Collectors.collectForceStringArr)

      case "ArrayType(CalendarIntervalType,false)" => newColumnCtx[Array[String]](Collectors.collectForceStringArr)

      case "NullType"             => newColumnCtx[String](Collectors.collectForceString)

      case _                      => newColumnCtx[Any](Collectors.collectDefault)
    }
  }
}
