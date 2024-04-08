package sparklyr

object StructTypeAsJSON {
  final val ReStructType = """(StructType\(.*\)|ArrayType\(.*StructType\(.*\).*\))""".r
  final val DType: String = "SparklyrStructTypeAsJSON"
}

// this class wraps a JSON string that will be deserialized into a vector or a
// list in R
class StructTypeAsJSON(val json: String) {
}
