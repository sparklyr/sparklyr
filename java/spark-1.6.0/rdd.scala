package sparklyr

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

class WorkerRDD(
  prev: RDD[Row],
  closure: Array[Byte],
  columns: Array[String],
  config: String,
  port: Int,
  groupBy: Array[String],
  closureRLang: Array[Byte],
  bundlePath: String,
  customEnv: Map[String, String],
  connectionTimeout: Int
  ) extends RDD[Row](prev) {

  private[this] var exception: Option[Exception] = None
  private[this] var backendPort: Int = 0

  override def getPartitions = firstParent.partitions

  override def compute(split: Partition, task: TaskContext): Iterator[Row] = {

    val sessionId: Int = scala.util.Random.nextInt(10000)
    val logger = new Logger("Worker", sessionId)
    val lock: AnyRef = new Object()

    val workerContext = new WorkerContext(
      firstParent.iterator(split, task).toArray,
      lock,
      closure,
      columns,
      groupBy,
      closureRLang,
      bundlePath
    )

    val contextId = JVMObjectTracker.put(workerContext)
    logger.log("is tracking worker context under " + contextId)

    logger.log("initializing backend")
    val backend: Backend = new Backend()

    /*
     * initialize backend as worker and service, since exceptions and
     * terminating the r session should not shutdown the process
     */
    backend.setType(
      true,   /* isService */
      false,  /* isRemote */
      true    /* isWorker */
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
          rscript.init(
            sessionId,
            backendPort,
            config,
            customEnv
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
