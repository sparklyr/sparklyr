package sparklyr

object Test {
  def nullary() = 0

  def unary_primitive_int(i : Int) = i * i
  def unary_Integer(i : Integer) = i * i
  def unary_nullable_Integer(i : Integer) = Option(i) match {
    case None => -1
    case Some(j) => j * j
  }

  def unary_seq(xs : Seq[Double]) = xs.map(x => x * x).sum
  def unary_nullable_seq(xs : Seq[Double]) = Option(xs) match {
    case None => -1
    case Some(ys) => ys.map(y => y * y).sum
  }

  def infer(x: Double) = "Double"
  def infer(s: String) = "String"
  def infer(xs: Seq[Double]) = "Seq"
}
