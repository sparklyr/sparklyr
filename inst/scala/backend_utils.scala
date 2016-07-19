package sparklyr

import java.io._
import java.io.File
import java.util.Arrays

import org.apache.spark.{SparkEnv, SparkException}

object BackendUtils {
  var rPackages: Option[String] = None

  /**
   * Return a nice string representation of the exception. It will call "printStackTrace" to
   * recursively generate the stack trace including the exception and its causes.
   */
  def exceptionString(e: Throwable): String = {
    if (e == null) {
      ""
    } else {
      // Use e.printStackTrace here because e.getStackTrace doesn't include the cause
      val stringWriter = new StringWriter()
      e.printStackTrace(new PrintWriter(stringWriter))
      stringWriter.toString
    }
  }
}
