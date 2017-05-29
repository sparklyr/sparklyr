package SparkWorker

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect.ClassTag

import sparklyr.Backend
import sparklyr.Logger

class WorkerContext[T: ClassTag](
  rdd: RDD[T],
  split: Partition,
  task: TaskContext,
  lock: AnyRef) {

  private var result: Array[T] = Array[T]()

  def getSourceIterator(): Iterator[T] = {
    rdd.iterator(split, task)
  }

  def getSourceArray(): Array[T] = {
    getSourceIterator.toArray
  }

  def getSourceArrayLength(): Int = {
    getSourceIterator.toArray.length
  }

  def getSourceArraySeq(): Array[Seq[Any]] = {
    getSourceArray.map(x => x.asInstanceOf[Row].toSeq)
  }

  def setResultArray(resultParam: Array[T]) = {
    result = resultParam
  }

  def setResultArraySeq(resultParam: Array[Any]) = {
    result = resultParam.map(x => Row.fromSeq(x.asInstanceOf[Array[Any]].toSeq).asInstanceOf[T])
  }

  def getResultArray(): Array[T] = {
    result
  }

  def finish(): Unit = {
    lock.synchronized {
      lock.notify
    }
  }
}

object WorkerRDD {
  private var context: Option[AnyRef] = None

  def setContext(contextParam: AnyRef) = {
    context = Some(contextParam)
  }

  def getContext(): AnyRef = {
    context.get
  }
}

class WorkerRDD[T: ClassTag](parent: RDD[T], sessionId: Int)
  extends RDD[T](parent) {

  private[this] var port: Int = 8880

  override def getPartitions = parent.partitions

  override def compute(split: Partition, task: TaskContext): Iterator[T] = {

    val logger = new Logger("Worker", sessionId)
    val lock: AnyRef = new Object()

    val workerContext = new WorkerContext[T](
      parent,
      split,
      task,
      lock
    )

    WorkerRDD.setContext(workerContext)

    new Thread("starting backend thread") {
      override def run(): Unit = {
        try {
          logger.log("Backend starting")
          val backend: Backend = new Backend()
          backend.init(port, sessionId, false, false, true)
        } catch {
          case e: Exception =>
            logger.logError("Failed to start backend: ", e)
        }
      }
    }.start()

    new Thread("starting rscript thread") {
      override def run(): Unit = {
        try {
          logger.log("RScript starting")

          val rscript = new Rscript(logger)
          rscript.init(sessionId)
        } catch {
          case e: Exception =>
            logger.logError("Failed to start rscript: ", e)
        }
      }
    }.start()

    lock.synchronized {
      lock.wait()
    }

    return workerContext.getResultArray().iterator
  }
}

object WorkerHelper {
  def computeRdd(df: DataFrame): RDD[Row] = {

    val sessionId = scala.util.Random.nextInt(10000)
    val logger = new Logger("Worker", sessionId)
    logger.log("RDD compute starting")

    val parent: RDD[Row] = df.rdd
    val computed: RDD[Row] = new WorkerRDD[Row](parent, sessionId)

    computed
  }
}
