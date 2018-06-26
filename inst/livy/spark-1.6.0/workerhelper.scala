//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

object WorkerHelper {
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql._
  import org.apache.spark.sql.catalyst.encoders.RowEncoder
  import org.apache.spark.sql.types._
  import scala.collection.JavaConversions._

  def computeRdd(
    rdd: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row],
    closure: Array[Byte],
    config: String,
    port: Int,
    columns: Array[String],
    groupBy: Array[String],
    closureRLang: Array[Byte],
    bundlePath: String,
    customEnv: java.util.Map[Object, Object],
    connectionTimeout: Int,
    context: Array[Byte],
    options: java.util.Map[Object, Object]
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

    val computed: RDD[Row] = new WorkerRDD(
      rdd,
      closure,
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

    computed
  }

  def computeSdf(
    sdf: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
    schema: StructType,
    closure: Array[Byte],
    config: String,
    port: Int,
    columns: Array[String],
    groupBy: Array[String],
    closureRLang: Array[Byte],
    bundlePath: String,
    customEnv: java.util.Map[Object, Object],
    connectionTimeout: Int,
    context: Array[Byte],
    options: java.util.Map[Object, Object]
  ): org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = {

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

    val encoder = RowEncoder(schema)

    sdf.mapPartitions(rows => {
      val workerApply: WorkerApply = new WorkerApply(
        closure,
        columns,
        config,
        port,
        groupBy,
        closureRLang,
        bundlePath,
        customEnvImmMap,
        connectionTimeout,
        context,
        optionsImmMap
      )

      workerApply.apply(rows)
    })(encoder)
  }
}
