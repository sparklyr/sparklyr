package sparklyr

import java.net._

import org.apache.spark._
import org.apache.spark.BarrierTaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.JavaConversions._

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
      options: Map[Object, Object]
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

    val workerApply = barrierApply(closure,
      columns,
      config,
      port,
      groupBy,
      closureRLang,
      bundlePath,
      customEnvImmMap,
      connectionTimeout,
      context,
      optionsImmMap)

    rdd.barrier.mapPartitions(workerApply.apply)
  }

  def barrierApply(
      closure: Array[Byte],
      columns: Array[String],
      config: String,
      port: Int,
      groupBy: Array[String],
      closureRLang: Array[Byte],
      bundlePath: String,
      customEnvImmMap: Map[String, String],
      connectionTimeout: Int,
      context: Array[Byte],
      optionsImmMap: Map[String, String]): sparklyr.WorkerApply = {
    val workerApply: WorkerApply = new WorkerApply(
      closure: Array[Byte],
      columns: Array[String],
      config: String,
      port: Int,
      groupBy: Array[String],
      closureRLang: Array[Byte],
      bundlePath: String,
      customEnvImmMap: Map[String, String],
      connectionTimeout: Int,
      context: Array[Byte],
      optionsImmMap: Map[String, String],
      "",
      org.apache.spark.sql.types.StructType(Nil),
      () => Map(
        "address" -> BarrierTaskContext.get().getTaskInfos().map(e => e.address),
        "partition" -> BarrierTaskContext.get().partitionId()),
      () => { BarrierTaskContext.get().partitionId() }
    )

    workerApply
    }

}
