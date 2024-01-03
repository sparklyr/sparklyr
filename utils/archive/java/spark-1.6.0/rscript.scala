package sparklyr

import java.io.File
import java.util.Arrays

import org.apache.spark._
import org.apache.spark.sql._

import scala.collection.JavaConverters._

class Rscript(logger: Logger) {
  def getScratchDir(): File = {
    new File(SparkFiles.getRootDirectory)
  }

  def getCommand(): String = {
    SparkEnv.get.conf.get("spark.r.command", "Rscript")
  }

  def run(commands: String) = {
    val processBuilder: ProcessBuilder = new ProcessBuilder(commands.split(" ").toList.asJava)

    processBuilder.redirectErrorStream(true);
    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    processBuilder.directory(getScratchDir());

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
    params: List[String],
    sourceFilePath: String,
    customEnv: Map[String, String],
    options: Map[String, String]) = {

    val command: String = getCommand()

    logger.log("using source file " + sourceFilePath)

    if (options.contains("rscript.before")) {
      run(options.getOrElse("rscript.before", ""))
    }

    val vanilla = options.getOrElse("vanilla", "true") == "true"

    val commandParams = (List(
      command,
      if (vanilla) "--vanilla" else "",
      sourceFilePath
    ) ++ params).filter(_.nonEmpty)

    logger.log("launching command " + commandParams.mkString(" "))

    val processBuilder: ProcessBuilder = new ProcessBuilder(commandParams.asJava)

    val processEnv = processBuilder.environment();
    for ((k, v) <- customEnv) {
      logger.log("is adding env var " + k + " and value " + v)
      processEnv.put(k, v)
    }

    processBuilder.redirectErrorStream(true);
    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    processBuilder.directory(getScratchDir());

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

