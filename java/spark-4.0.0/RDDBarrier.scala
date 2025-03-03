package sparklyr

import java.net._

import org.apache.spark._
import org.apache.spark.BarrierTaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.jdk.CollectionConverters._

object RDDBarrier {
  def transformBarrier(
    rdd: RDD[Row],
    closure: Array[Byte],
    columns: Array[String],
    config: String,
    port: Int,
    groupBy: Array[String],
    closureRLang: Array[Byte],
    bundlePath: String,
    connectionTimeout: Int,
    customEnv: Map[Object, Object],
    context: Array[Byte],
    options: Map[Object, Object],
    serializer: Array[Byte],
    deserializer: Array[Byte]
  ): RDD[Row] = {

    var customEnvMap = scala.collection.mutable.Map[String, String]();
    customEnv.foreach(kv => customEnvMap.put(
      kv._1.asInstanceOf[String],
      kv._2.asInstanceOf[String])
    )

    var optionsMap = scala.collection.mutable.Map[String, String]();
    options.foreach(kv => optionsMap.put(
      kv._1.asInstanceOf[String],
      kv._2.asInstanceOf[String])
    )

    val customEnvImmMap = (Map() ++ customEnvMap).toMap
    val optionsImmMap = (Map() ++ optionsMap).toMap
    val sparkContext = rdd.context

    val workerApply = barrierApply(
     sparkContext.broadcast(closure),
      columns,
      config,
      port,
      groupBy,
      sparkContext.broadcast(closureRLang),
      bundlePath,
      customEnvImmMap,
      connectionTimeout,
      sparkContext.broadcast(context),
      optionsImmMap,
      sparkContext.broadcast(serializer),
      sparkContext.broadcast(deserializer)
    )

    rdd.barrier.mapPartitions(workerApply.apply)
  }

  def barrierApply(
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
  ): sparklyr.WorkerApply = {
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
      schema = org.apache.spark.sql.types.StructType(Nil),
      barrierMapProvider = () => Map(
        "address" -> BarrierTaskContext.get().getTaskInfos().map(e => e.address),
        "partition" -> BarrierTaskContext.get().partitionId()),
      partitionIndexProvider = () => { BarrierTaskContext.get().partitionId() },
      serializer = deserializer,
      deserializer = deserializer
    )

    workerApply
  }
}
