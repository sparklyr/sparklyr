package sparklyr

import org.apache.spark._

object WorkerMap {

  import org.apache.spark.sql._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.catalyst.encoders.RowEncoder

  def mapPartitions(input: Dataset[Row]): Dataset[Row] = {
    val schema = StructType(Seq(
      StructField("A", IntegerType),
      StructField("B", IntegerType),
      StructField("C", IntegerType)
    ))

    val encoder = RowEncoder(schema)

    input.mapPartitions(lines => {
      Iterator(Row(1,2,3), Row(10,20,30))
    })(encoder)
  }
}
