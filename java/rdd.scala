package SparkWorker

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect.ClassTag

import sparklyr.Backend

import Logging._

object WorkerRDD {
  private var split: Option[Partition] = None;

  def hasSplit: Boolean = !split.isEmpty;
  def getSplit: Partition = split.get;
}

class WorkerRDD[T: ClassTag](parent: RDD[T])
  extends RDD[Row](parent) {

  private[this] var port: Int = 8880
  private[this] var sessionId: Int = scala.util.Random.nextInt(100)

  override def getPartitions = parent.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {

    WorkerRDD.split = Some(split);

    new Thread("start sparklyr backend thread") {
      override def run(): Unit = {
        try {
          Logging.log("Backend starting")
          val backend: Backend = new Backend()
          backend.init(port, sessionId, false, false, true)
        } catch {
          case e: Exception =>
            Logging.logError("Failed to start backend: ", e)
        }
      }
    }.start()

    Process.init(sessionId)

    return new Iterator[Row] {
      def next(): Row = org.apache.spark.sql.Row.fromSeq(Array[String]())
      def hasNext = false
    }
  }
}

object WorkerHelper {
  def computeRdd(df: DataFrame): RDD[Row] = {
    log("RDD compute starting")

    val parent: RDD[Row] = df.rdd
    val computed: RDD[Row] = new WorkerRDD[Row](parent)

    computed
  }
}
