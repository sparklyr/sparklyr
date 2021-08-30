package sparklyr

import scala.util.Try

object Shell {
  private[this] var backend: Backend = null

  /**
   * Main method and entry point of the application.
   */
  def main(args: Array[String]): Unit = {
    main(args, List.empty)
  }

  /**
   * Overloads the main method. In order to use this, it must be directly invoked.
   * @param args command-line arguments
   * @param preCommandHooks functions that are executed by the backend before handling each method call
   */
  def main(args: Array[String], preCommandHooks: List[Runnable]): Unit = {
    val isService = args.contains("--service")
    val isRemote = args.contains("--remote")
    val isBatch = args.contains("--batch")

    if ((!isBatch && args.length > 4) || args.length < 2) {
      System.err.println(
        "Usage: Backend port id [--service] [--remote] [--batch file.R]\n" +
        "  port:      port the gateway will listen to\n" +
        "  id:        arbitrary numeric identifier for this backend session\n" +
        "  --service: prevents closing the connection from closing the backend\n" +
        "  --remote:  allows the gateway to accept remote connections\n" +
        "  --batch:   local path to R file to be executed in batch mode\n"
      )

      System.exit(-1)
    }

    val port = args(0).toInt
    val sessionId = args(1).toInt
    val connectionTimeout = if (args.length <= 2) 60 else scala.util.Try(args(2).toInt).toOption.getOrElse(60)

    var batchFile = ""
    if (isBatch) batchFile = args(args.indexOf("--batch") + 1)

    backend = new Backend()
    backend.setType(isService, isRemote, false, isBatch)
    backend.setArgs(args)
    backend.setPreCommandHooks(preCommandHooks)
    backend.init(port, sessionId, connectionTimeout, batchFile)
  }

  def getBackend(): Backend = {
    backend
  }
}
