package sparklyr

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StructType, DataType}

object SQLUtils3 {
  def createDataFrame(
    spark: SparkSession,
    catalystRows: RDD[InternalRow],
    schema: Any // Accept both StructType and String
  ): DataFrame = {

    // Convert schema if it's a string
    val structSchema: StructType = schema match {
      case s: StructType => s
      case s: String =>
        try {
          DataType.fromJson(s).asInstanceOf[StructType] // JSON schema
        } catch {
          case _: Exception => StructType.fromDDL(s) // DDL schema
        }
      case _ => throw new IllegalArgumentException("Invalid schema format")
    }

    // Convert InternalRow to Row
    val rowRDD: RDD[Row] = catalystRows.map { internalRow =>
      val values = structSchema.fields.indices.map { i =>
        internalRow.get(i, structSchema.fields(i).dataType)
      }
      Row(values: _*)
    }

    // Create DataFrame
    spark.createDataFrame(rowRDD, structSchema)
  }
}
