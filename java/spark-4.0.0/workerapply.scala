package sparklyr

import java.io.{File, FileWriter}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType
import scala.util.Random

class WorkerApply(
  closure: Broadcast[Array[Byte]],
  columns: Array[String],
  config: String,
  port: Int,
  groupBy: Array[String],
  closureRLang: Broadcast[Array[Byte]],
  bundlePath: String,
  customEnv: Map[String, String],
  connectionTimeout: Int,
  context: Broadcast[Array[Byte]],
  options: Map[String, String],
  timeZoneId: String,
  schema: StructType,
  barrierMapProvider: () => Map[String, Any],
  partitionIndexProvider: () => Int,
  serializer: Broadcast[Array[Byte]],
  deserializer: Broadcast[Array[Byte]]
) extends Serializable {

  @transient private[this] var exception: Option[Exception] = None
  @transient private[this] var backendPort: Int = 0

  def workerSourceFile(rscript: Rscript, sessionId: Int): String = {
    val source = Sources.sources
    val tempFile = new File(s"${rscript.getScratchDir()}${File.separator}sparkworker_$sessionId.R")
    val outStream = new FileWriter(tempFile)
    outStream.write(source)
    outStream.flush()
    outStream.close()
    tempFile.getAbsolutePath
  }

  def apply(iterator: Iterator[Row]): Iterator[Row] = {
    val sessionId = Random.nextInt(10000)
    val logger = new Logger("Worker", sessionId)
    val lock = new Object()

    if (!iterator.hasNext) return Iterator.empty

    val workerContext = new WorkerContext(
      iterator,
      lock,
      closure.value,
      columns,
      groupBy,
      closureRLang.value,
      bundlePath,
      context.value,
      timeZoneId,
      schema,
      options,
      barrierMapProvider(),
      partitionIndexProvider(),
      serializer.value,
      deserializer.value
    )

    val tracker = new JVMObjectTracker()
    val contextId = tracker.put(workerContext)
    logger.log(s"is tracking worker context under $contextId")

    logger.log("initializing backend")
    val backend = new Backend()
    backend.setTracker(tracker)
    backend.setType(
      true,   /* isService */
      false,  /* isRemote */
      true,   /* isWorker */
      false   /* isBatch */
    )
    backend.setHostContext(contextId)
    backend.init(port, sessionId, connectionTimeout)
    backendPort = backend.getPort()

    val backendThread = new Thread("backend thread") {
      override def run(): Unit = {
        try {
          logger.log("starting backend")
          backend.run()
        } catch {
          case e: Exception =>
            logger.logError("failed while running backend: ", e)
            exception = Some(e)
            lock.synchronized {
              lock.notify()
            }
        }
      }
    }
    backendThread.setDaemon(true)
    backendThread.start()

    val rscriptThread = new Thread("rscript thread") {
      override def run(): Unit = {
        try {
          logger.log("starting rscript")
          val rscript = new Rscript(logger)
          val sourceFilePath = workerSourceFile(rscript, sessionId)
          rscript.init(
            List(sessionId.toString, backendPort.toString, config),
            sourceFilePath,
            customEnv,
            options
          )
          lock.synchronized {
            lock.notify()
          }
        } catch {
          case e: Exception =>
            logger.logError("failed to run rscript: ", e)
            exception = Some(e)
            lock.synchronized {
              lock.notify()
            }
        }
      }
    }
    rscriptThread.setDaemon(true)
    rscriptThread.start()

    logger.log("waiting for RScript to complete")
    lock.synchronized {
      lock.wait()
    }
    logger.log("RScript completed")

    exception match {
      case Some(x) => throw x
      case _    => println("")
    }

    logger.log(s"returning RDD iterator with ${workerContext.getResultArray().length} rows")
    workerContext.getResultArray().iterator
  }
}
