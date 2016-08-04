package sparklyr

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date

object Logging {
  def getDate() : String = {
    val now = Calendar.getInstance().getTime()
    val logFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss")
    return logFormat.format(now)
  }

  def logError(message: String) = {
    System.err.println(getDate() + " ERROR " + message)
  }

  def logError(message: String, e: Exception) = {
    System.err.println(getDate() + " ERROR " + message, e.toString)
  }

  def logWarning(message: String) = {
    System.err.println(getDate() + " WARN " + message)
  }
}
