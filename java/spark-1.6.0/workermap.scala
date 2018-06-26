package sparklyr

import org.apache.spark._

object WorkerMap {

  import org.apache.spark.sql._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.catalyst.encoders.RowEncoder

  def mapPartitions(
    input: Dataset[Row],
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
    schema: StructType
  ): Dataset[Row] = {
    val encoder = RowEncoder(schema)

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

    input.mapPartitions(lines => {
      workerApply.apply(lines)
    })(encoder)
  }
}
