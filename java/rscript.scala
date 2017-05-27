package SparkWorker

import java.io.{File, FileWriter}
import java.util.Arrays

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import ClassUtils._
import FileUtils._

import sparklyr.Logger

class Rscript(logger: Logger) {
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
    logger.log("Path to source file " + sourceFilePath)

    val processBuilder: ProcessBuilder = new ProcessBuilder(Arrays.asList(
      command,
      "--vanilla",
      sourceFilePath,
      sessionId.toString
    ))

    processBuilder.redirectErrorStream(true);
    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);

    logger.log("R process starting")
    val process: Process = processBuilder.start()
    val status: Int = process.waitFor()

    if (status == 0) {
      logger.log("R process completed")
    } else {
      logger.logError("R process failed")
      throw new Exception(s"sparklyr worker rscript failure, check worker logs for details.")
    }
  }
}

