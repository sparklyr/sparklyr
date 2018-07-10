package sparklyr

import org.apache.spark._

class WorkerRDD(
  prev: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row],
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
  options: Map[String, String]
  ) extends org.apache.spark.rdd.RDD[org.apache.spark.sql.Row](prev) {

  import org.apache.spark._;

  override def getPartitions = firstParent.partitions

  override def compute(split: Partition, task: TaskContext): Iterator[org.apache.spark.sql.Row] = {

    val workerApply: WorkerApply = new WorkerApply(
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
      options: Map[String, String]
    )

    return workerApply.apply(firstParent.iterator(split, task))
  }
}
