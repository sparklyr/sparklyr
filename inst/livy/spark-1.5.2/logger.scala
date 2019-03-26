class Logger(role: String, id: Int) {
  def logLevel(level: String, message: String): Unit = {
    var line = ""
    val format = new java.text.SimpleDateFormat("yy/MM/dd HH:mm:ss")
    line += format.format(new java.util.Date())
    line += " "
    line += level
    line += " sparklyr: "
    line += role
    line += " ("
    line += id
    line += ") "
    line += message

    scala.Console.println(line)
  }

  def log(message: String): Unit = {
    logLevel("INFO", message)
  }

  def log(message: String, e: Exception): Unit = {
    log(message + ": " + e.getMessage())
    e.printStackTrace()
  }

  def logError(message: String): Unit = {
    logLevel("INFO", message)
  }

  def logError(message: String, e: Exception): Unit = {
    logLevel("ERROR", message)
  }

  def logWarning(message: String): Unit = {
    logLevel("WARN", message)
  }
}
