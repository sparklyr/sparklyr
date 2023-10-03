package sparklyr

class Rscript(logger: Logger) {
  import java.io.File

  def getScratchDir(): File = {
    new File("")
  }

  def init(
    params: List[String],
    sourceFilePath: String,
    customEnv: Map[String, String],
    options: Map[String, String]) = {
  }
}

