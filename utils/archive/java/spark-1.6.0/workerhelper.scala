package sparklyr

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

object WorkerHelper {
  def computeRdd(
    rdd: RDD[Row],
    closure: Array[Byte],
    config: String,
    port: Int,
    columns: Array[String],
    groupBy: Array[String],
    closureRLang: Array[Byte],
    bundlePath: String,
    customEnv: Map[_, _],
    connectionTimeout: Int,
    context: Array[Byte],
    options: Map[_, _],
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

    val computed: RDD[Row] = new WorkerRDD(
      rdd,
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

    computed
  }
}
