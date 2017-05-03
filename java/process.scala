package SparkWorker

import java.io.{File, FileWriter}
import java.util.Arrays

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import ClassUtils._
import FileUtils._
import Logging._

object Process {
  def workerSourceFile(): String = {
    val source = Embedded.sources

    val tempFile: File = new File(createTempDir + File.separator + "sparkworker.R")
    val outStream: FileWriter = new FileWriter(tempFile)
    outStream.write(source)
    outStream.flush()

    tempFile.getAbsolutePath()
  }

  def init(sessionId: Int) = {
    val sparkConf = SparkEnv.get.conf
    val command: String = sparkConf.get("spark.r.command", "Rscript")

    val sourceFilePath: String = workerSourceFile()
    log("Path to source file " + sourceFilePath)

    val processBuilder = new ProcessBuilder(Arrays.asList(
      command,
      "--vanilla",
      sourceFilePath,
      sessionId
    ))

    if (classExists("sparklyr.Backend")) {
      log("sparklyr.Backend exists")
    }

    log("R process starting")
    processBuilder.start()
  }
}

