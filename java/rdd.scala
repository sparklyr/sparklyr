package SparkWorker

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect.ClassTag

class WorkerRDD[T: ClassTag](parent: RDD[T])
  extends RDD[Row](parent) {

  override def getPartitions = parent.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    Process.start()

    return new Iterator[Row] {
      def next(): Row = org.apache.spark.sql.Row.fromSeq(Array[String]())
      def hasNext = false
    }
  }
}

object WorkerHelper {
  def computeRdd(df: DataFrame): RDD[Row] = {
    val parent: RDD[Row] = df.rdd
    val computed: RDD[Row] = new WorkerRDD[Row](parent)

    computed
  }
}
