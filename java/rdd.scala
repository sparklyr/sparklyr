package SparkWorker

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect.ClassTag

import sparklyr.Backend

import Logging._

class WorkerRDD[T: ClassTag](parent: RDD[T])
  extends RDD[Row](parent) {

  private[this] var port: Int = 8880
  private[this] var sessionId: Int = scala.util.Random.nextInt(100)

  override def getPartitions = parent.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {

    new Thread("start sparklyr backend thread") {
      override def run(): Unit = {
        Backend.init(port, sessionId, false, false)
      }
    }.start()

    Process.init()

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
