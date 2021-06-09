package sparklyr

case class Logical(intValue: Int) // 0 -> FALSE, 1 -> TRUE, or scala.Int.MinValue -> NA
case class Numeric(value: Option[Double])
case class Raw(value: Array[Byte])
