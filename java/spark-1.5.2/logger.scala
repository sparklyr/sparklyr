package sparklyr

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date

class Logger(role: String, id: Int)  {
  def getDate() : String = {
    val now = Calendar.getInstance().getTime()
    val logFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss")
    return logFormat.format(now)
  }

  def log(message: String) = {
    System.out.println(getDate() + " INFO sparklyr: " + role + " (" + id + ") " + message)
  }

  def logError(message: String) = {
    System.err.println(getDate() + " ERROR sparklyr: " + role + " (" + id + ") " + message)
  }

  def logError(message: String, e: Exception) = {
    System.err.println(getDate() + " ERROR sparklyr: " + role + " (" + id + ") " + message, e.toString)
  }

  def logWarning(message: String) = {
    System.err.println(getDate() + " WARN sparklyr: " + role + " (" + id + ") " + message)
  }
}
