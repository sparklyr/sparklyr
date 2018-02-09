package sparklyr

import java.io.{File, FileWriter}
import java.util.Arrays

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.collection.JavaConverters._

import ClassUtils._
import FileUtils._

class Rscript(logger: Logger) {
  val scratchDir: File = new File(SparkFiles.getRootDirectory)

  def workerSourceFile(): String = {
    val source = Sources.sources

    val tempFile: File = new File(scratchDir + File.separator + "sparkworker.R")
    val outStream: FileWriter = new FileWriter(tempFile)
    outStream.write(source)
    outStream.flush()

    tempFile.getAbsolutePath()
  }

  def run(commands: String) = {
    val processBuilder: ProcessBuilder = new ProcessBuilder(commands.split(" ").toList.asJava)

    processBuilder.redirectErrorStream(true);
    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    processBuilder.directory(scratchDir);

    logger.log("is running custom command: " + commands)
    val process: Process = processBuilder.start()
    val status: Int = process.waitFor()

    if (status == 0) {
      logger.log("completed custom command")
    } else {
      logger.logError("failed to complete custom command")
    }
  }

  def init(
    sessionId: Int,
    backendPort: Int,
    config: String,
    customEnv: Map[String, String],
    options: Map[String, String]) = {

    val sparkConf = SparkEnv.get.conf
    val command: String = sparkConf.get("spark.r.command", "Rscript")

    val sourceFilePath: String = workerSourceFile()
    logger.log("using source file " + sourceFilePath)

    if (options.contains("rscript.before")) {
      run(options.getOrElse("rscript.before", ""))
    }

    val vanilla = options.getOrElse("vanilla", "true") == "true"

    val commandParams = List(
      command,
      if (vanilla) "--vanilla" else "",
      sourceFilePath,
      sessionId.toString,
      backendPort.toString,
      config
    ).filter(_.nonEmpty)

    logger.log("launching command " + commandParams.mkString(" "))

    val processBuilder: ProcessBuilder = new ProcessBuilder(commandParams.asJava)

    val processEnv = processBuilder.environment();
    for ((k, v) <- customEnv) {
      logger.log("is adding env var " + k + " and value " + v)
      processEnv.put(k, v)
    }

    processBuilder.redirectErrorStream(true);
    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);

    logger.log("is starting R process")
    val process: Process = processBuilder.start()
    val status: Int = process.waitFor()

    if (status == 0) {
      logger.log("completed R process")
    } else {
      logger.logError("failed to complete R process")

      throw new Exception("sparklyr worker rscript failure with status " + status + ", check worker logs for details.")
    }
  }
}

