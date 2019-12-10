package sparklyr

object RDDBarrier {
  import org.apache.spark._
  import scala.collection.JavaConversions._
  import java.net._

  def transformBarrier(
      rdd: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row],
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
      ): org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = {

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
      true
    )

    workerApply
    }

}
