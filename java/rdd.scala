package SparkWorker

import org.apache.spark._
import org.apache.spark.rdd.RDD

class WorkerRDD[T: ClassManifest](parent: RDD[T])
  extends RDD[Array[Byte]](parent) {

  override def getPartitions = parent.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    return new Iterator[Array[Byte]] {
      def next(): Array[Byte] = new Array[Byte](0)
      def hasNext = false
    }
  }
}
