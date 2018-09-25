//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

class Rscript(logger: Logger) {
  import java.io.File
  import java.util.Arrays

  import org.apache.spark._
  import org.apache.spark.sql._

  import scala.collection.JavaConverters._

  def getScratchDir(): File = {
    val sparkFiles = Class.forName("org.apache.spark.SparkFiles")
    val selectedMethods = sparkFiles.getMethods.filter(m => m.getName == "getRootDirectory")
    val rootDir = selectedMethods(0).invoke(sparkFiles).asInstanceOf[String]
    new File(rootDir)
  }

  def getSparkConf(): SparkConf = {
    // val sparkConf = SparkEnv.get.conf
    val sparkEnvRoot = Class.forName("org.apache.spark.SparkEnv")
    val selectedMethods = sparkEnvRoot.getMethods.filter(
      m => m.getName == "get" && m.getParameterTypes.length == 0
    )
    val sparkEnv: SparkEnv= selectedMethods(0).invoke(sparkEnvRoot).asInstanceOf[SparkEnv]

    val sparkEnvMethods = sparkEnv.getClass.getMethods.filter(
      m => m.getName == "conf" && m.getParameterTypes.length == 0
    )
    sparkEnvMethods(0).invoke(sparkEnv).asInstanceOf[SparkConf]
  }

  def getCommand(): String = {
    val sparkConf: SparkConf = getSparkConf()

    // val command: String = sparkConf.get("spark.r.command", "Rscript")
    val sparkConfMethods = sparkConf.getClass.getMethods.filter(
      m => m.getName == "get" && m.getParameterTypes.length == 2
    )
    sparkConfMethods(0).invoke(sparkConf, "spark.r.command", "Rscript").asInstanceOf[String]
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

