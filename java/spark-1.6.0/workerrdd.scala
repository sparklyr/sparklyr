package sparklyr

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

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
  connectionTimeout: Int,
  context: Array[Byte],
  options: Map[String, String]
  ) extends RDD[Row](prev) {

  override def getPartitions = firstParent.partitions

  override def compute(split: Partition, task: TaskContext): Iterator[Row] = {

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
      options: Map[String, String],
      "",
      StructType(Nil),
      () => Map()
    )

    return workerApply.apply(firstParent.iterator(split, task))
  }
}
