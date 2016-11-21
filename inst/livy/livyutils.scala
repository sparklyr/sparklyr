// Base64 requires Java 8
import java.util.Base64

import sparklyr.StreamHandler._

object LivyUtils {
  def invokeFromBase64(msg: String): String = {
    val decoded: Array[Byte] = Base64.getDecoder().decode(msg)
    val result = StreamHandler.read(decoded)

    Base64.getEncoder().encodeToString(result);
  }
}
