package SparkWorker

import java.util.Arrays

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import Logging._

object Process {
  def start() = {
    val sparkConf = SparkEnv.get.conf
    val command = sparkConf.get("spark.r.command", "Rscript")

    val script = "" +
      "log_file <-  file.path(\"~\", \"spark\", basename(tempfile(fileext = \".log\")))" +
      "log <- function(message) {\n" +
      "   write(log_file, message)\n" +
      "}\n" +
      "\n" +
      "log(\"sparklyr worker starting\")\n" +
      "log(\"sparklyr worker finished\")\n"

    val processBuilder = new ProcessBuilder(Arrays.asList(
      command,
      "--vanilla",
      script
    ))

    log("R process starting")
    processBuilder.start()
  }
}

