package sparklyr

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

object StreamUtils {
  def lag(
    input: DataFrame,
    srcCols: Array[String],
    offsets: Array[Int],
    dstCols: Array[String]
  ): DataFrame = {
    val inputSchema = input.schema
    val fieldIdxes = srcCols.map(col => inputSchema.fieldIndex(col))
    val outputSchema = StructType(
      inputSchema.fields ++ (
        fieldIdxes.zipWithIndex.map{
          case (fieldIdx, idx) => StructField(
            name = dstCols(idx),
            dataType = inputSchema.apply(fieldIdx).dataType,
            nullable = true
          )
        }
      )
    )

    val encoder = RowEncoder(outputSchema)
    input.mapPartitions(
      iter => {
        val prevCols = new Array[Array[Any]](srcCols.length)
        for (i <- 0 until srcCols.length)
          prevCols(i) = new Array[Any](offsets(i))

        var rowNum: Int = 0
        val processRows: (Row) => Row = {
          row => {
            val vals = row.toSeq ++ (
              (0 until srcCols.length).map(
                i => prevCols(i)(rowNum % offsets(i))
              )
            )

            for (i <- 0 until srcCols.length)
              prevCols(i)(rowNum % offsets(i)) = row.apply(fieldIdxes(i))
            rowNum += 1

            new GenericRowWithSchema(
              values = vals.toArray,
              schema = outputSchema
            )
          }
        }
        iter.map(processRows)
      }
    )(encoder)
  }
}
