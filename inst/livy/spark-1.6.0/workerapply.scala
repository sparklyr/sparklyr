//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

class WorkerApply(
  closure: Array[Byte],
  columns: Array[String],
  config: String,
  port: Int,
  groupBy: Array[String],
  closureRLang: Array[Byte],
  bundlePath: String,
  customEnv: Map[String, String],
  connectionTimeout: Int,
  context: Array[Byte],
  options: Map[String, String],
  timeZoneId: String,
  schema: org.apache.spark.sql.types.StructType
  ) {

  import java.io.{File, FileWriter}
  import org.apache.spark._;

  private[this] var exception: Option[Exception] = None
  private[this] var backendPort: Int = 0

  def workerSourceFile(rscript: Rscript): String = {
    val rsources = new Sources()
    val source = rsources.sources

    val tempFile: File = new File(rscript.getScratchDir() + File.separator + "sparkworker.R")
    val outStream: FileWriter = new FileWriter(tempFile)
    outStream.write(source)
    outStream.flush()

    tempFile.getAbsolutePath()
  }

  def apply(iterator: Iterator[org.apache.spark.sql.Row]): Iterator[org.apache.spark.sql.Row] = {

    val sessionId: Int = scala.util.Random.nextInt(10000)
    val logger = new Logger("Worker", sessionId)
    val lock: AnyRef = new Object()

    // No point in starting up R process to not process anything
    if (!iterator.hasNext) return Array[org.apache.spark.sql.Row]().iterator

    val workerContext = new WorkerContext(
      iterator,
      lock,
      closure,
      columns,
      groupBy,
      closureRLang,
      bundlePath,
      context,
      timeZoneId,
      schema,
      options
    )

    val tracker = new JVMObjectTracker()
    val contextId = tracker.put(workerContext)
    logger.log("is tracking worker context under " + contextId)

    logger.log("initializing backend")
    val backend: Backend = new Backend()
    backend.setTracker(tracker)

    /*
     * initialize backend as worker and service, since exceptions and
     * terminating the r session should not shutdown the process
     */
    backend.setType(
      true,   /* isService */
      false,  /* isRemote */
      true,   /* isWorker */
      false   /* isBatch */
    )

    backend.setHostContext(
      contextId
    )

    backend.init(
      port,
      sessionId,
      connectionTimeout
    )

    backendPort = backend.getPort()

    new Thread("starting backend thread") {
      override def run(): Unit = {
        try {
          logger.log("starting backend")

          backend.run()
        } catch {
          case e: Exception =>
            logger.logError("failed while running backend: ", e)
            exception = Some(e)
            lock.synchronized {
              lock.notify
            }
        }
      }
    }.start()

    new Thread("starting rscript thread") {
      override def run(): Unit = {
        try {
          logger.log("is starting rscript")

          val rscript = new Rscript(logger)
          val sourceFilePath: String = workerSourceFile(rscript)

          rscript.init(
            List(
              sessionId.toString,
              backendPort.toString,
              config
            ),
            sourceFilePath,
            customEnv,
            options
          )

          lock.synchronized {
            lock.notify
          }
        } catch {
          case e: Exception =>
            logger.logError("failed to run rscript: ", e)
            exception = Some(e)
            lock.synchronized {
              lock.notify
            }
        }
      }
    }.start()

    logger.log("is waiting using lock for RScript to complete")
    lock.synchronized {
      lock.wait()
    }
    logger.log("completed wait using lock for RScript")

    if (exception.isDefined) {
      throw exception.get
    }

    logger.log("is returning RDD iterator with " + workerContext.getResultArray().length + " rows")
    return workerContext.getResultArray().iterator
  }
}
