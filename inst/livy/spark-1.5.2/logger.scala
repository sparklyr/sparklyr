class Logger(role: String, id: Int) {
  def logLevel(level: String, message: String) = {
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

    Console.println(line)
  }

  def log(message: String) = {
    logLevel("INFO", message)
  }

  def log(message: String, e: Exception) = {
    log(message + ": " + e.getMessage())
    e.printStackTrace()
  }

  def logError(message: String) = {
    logLevel("INFO", message)
  }

  def logError(message: String, e: Exception) = {
    logLevel("ERROR", message)
  }

  def logWarning(message: String) = {
    logLevel("WARN", message)
  }
}
