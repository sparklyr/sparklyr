package SparkWorker

import java.io.{File, FileWriter}
import java.util.Arrays

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import FileUtils._
import Logging._

object Process {
  def workerSourceFile(): String = {
    val source = "" +
      "log_file <-  file.path(\"~\", \"spark\", basename(tempfile(fileext = \".log\")))\n" +
      "log <- function(message) {\n" +
      "   write(paste0(message, \"\\n\"), file = log_file)\n" +
      "   cat(\"sparkworker:\", message, \"\\n\")\n" +
      "}\n" +
      "\n" +
      "log(\"sparklyr worker starting\")\n" +
      "log(\"sparklyr worker finished\")\n"

    val tempFile: File = new File(createTempDir + File.separator + "sparkrworker.R")
    val outStream: FileWriter = new FileWriter(tempFile)
    outStream.write(source)
    outStream.flush()

    tempFile.getAbsolutePath()
  }

  def start() = {
    val sparkConf = SparkEnv.get.conf
    val command: String = sparkConf.get("spark.r.command", "Rscript")

    val sourceFilePath: String = workerSourceFile()
    log("Path to source file " + sourceFilePath)

    val processBuilder = new ProcessBuilder(Arrays.asList(
      command,
      "--vanilla",
      sourceFilePath
    ))

    log("R process starting")
    processBuilder.start()
  }
}

