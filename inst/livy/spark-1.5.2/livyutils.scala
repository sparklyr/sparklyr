import javax.xml.bind.DatatypeConverter

object LivyUtils {
  // A global class map is needed in Livy since classes defined from
  // the livy-repl become assigned to a generic package that can't be
  // accessed through reflection.
  val globalClassMap = Map(
    "Logger" -> new Logger("", 0),
    "Serializer" -> Serializer,
    "SQLUtils" -> SQLUtils,
    "StreamHandler" -> StreamHandler,
    "JVMObjectTracker" -> JVMObjectTracker,
    "Utils" -> Utils,
    "Repartition" -> Repartition
  )

  def invokeFromBase64(msg: String): String = {

    val decoded: Array[Byte] = DatatypeConverter.parseBase64Binary(msg)
    val result = StreamHandler.read(
      decoded,
      globalClassMap,
      new Logger("", 0),
      "")

    DatatypeConverter.printBase64Binary(result);
  }
}
