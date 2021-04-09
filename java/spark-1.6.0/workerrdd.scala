package sparklyr

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

class WorkerRDD(
  prev: RDD[Row],
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
  serializer: Broadcast[Array[Byte]],
  deserializer: Broadcast[Array[Byte]]
) extends RDD[Row](prev) {

  override def getPartitions = firstParent.partitions

  override def compute(split: Partition, task: TaskContext): Iterator[Row] = {

    val workerApply: WorkerApply = new WorkerApply(
      closure = closure,
      columns = columns,
      config = config,
      port = port,
      groupBy = groupBy,
      closureRLang = closureRLang,
      bundlePath = bundlePath,
      customEnv = customEnv,
      connectionTimeout = connectionTimeout,
      context = context,
      options = options,
      timeZoneId = "",
      schema = StructType(Nil),
      barrierMapProvider = () => Map(),
      partitionIndexProvider = () => { split.index },
      serializer = serializer,
      deserializer = deserializer
    )

    return workerApply.apply(firstParent.iterator(split, task))
  }
}
