// Base64 requires Java 8
import java.util.Base64

object LivyUtils {
  // A global class map is needed in Livy since classes defined from
  // the livy-repl become assigned to a generic package that can't be
  // accessed through reflection.
  val globalClassMap = Map(
    "Logging" -> Logging,
    "Serializer" -> Serializer,
    "SQLUtils" -> SQLUtils,
    "StreamHandler" -> StreamHandler,
    "JVMObjectTracker" -> JVMObjectTracker,
    "Utils" -> Utils
  )

  def invokeFromBase64(msg: String): String = {

    val decoded: Array[Byte] = Base64.getDecoder().decode(msg)
    val result = StreamHandler.read(decoded, globalClassMap)

    Base64.getEncoder().encodeToString(result);
  }
}
