//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

import javax.xml.bind.DatatypeConverter

object LivyUtils {
  // A global class map is needed in Livy since classes defined from
  // the livy-repl become assigned to a generic package that can't be
  // accessed through reflection.
  val globalClassMap = Map(
    "Logger" -> new Logger("", 0),
    "SQLUtils" -> SQLUtils,
    "Utils" -> Utils,
    "Repartition" -> Repartition,
    "ArrowHelper" -> ArrowHelper,
    "ArrowConverters" -> ArrowConverters,
    "ApplyUtils" -> ApplyUtils,
    "WorkerHelper" -> WorkerHelper,
    "MLUtils" -> MLUtils,
    "MLUtils2" -> MLUtils2,
    "BucketizerUtils" -> BucketizerUtils
   )

  val tracker = new JVMObjectTracker()
  val serializer = new Serializer(tracker)
  val streamHandler = new StreamHandler(serializer, tracker)

  def invokeFromBase64(msg: String): String = {

    val decoded: Array[Byte] = DatatypeConverter.parseBase64Binary(msg)
    val result = streamHandler.read(
      decoded,
      globalClassMap,
      new Logger("", 0),
      "")

    DatatypeConverter.printBase64Binary(result);
  }
}
