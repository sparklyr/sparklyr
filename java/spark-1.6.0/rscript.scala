package sparklyr

import java.io.{File, FileWriter}
import java.util.Arrays

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import ClassUtils._
import FileUtils._

class Rscript(logger: Logger) {
  def workerSourceFile(): String = {
    val source = Sources.sources

    val tempFile: File = new File(createTempDir + File.separator + "sparkworker.R")
    val outStream: FileWriter = new FileWriter(tempFile)
    outStream.write(source)
    outStream.flush()

    tempFile.getAbsolutePath()
  }

  def init(sessionId: Int, backendPort: Int, config: String) = {
    val sparkConf = SparkEnv.get.conf
    val command: String = sparkConf.get("spark.r.command", "Rscript")

    val sourceFilePath: String = workerSourceFile()
    logger.log("using source file " + sourceFilePath)
    logger.log(
      "launching command " + command + " --vanilla <source-file> " +
      sessionId.toString + " " + config)

    val processBuilder: ProcessBuilder = new ProcessBuilder(Arrays.asList(
      command,
      "--vanilla",
      sourceFilePath,
      sessionId.toString,
      backendPort.toString,
      config
    ))

    processBuilder.redirectErrorStream(true);
    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);

    logger.log("is starting R process")
    val process: Process = processBuilder.start()
    val status: Int = process.waitFor()

    if (status == 0) {
      logger.log("completed R process")
    } else {
      logger.logError("failed to complete R process")

      throw new Exception(s"sparklyr worker rscript failure, check worker logs for details.")
    }
  }
}

