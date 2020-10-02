package sparklyr

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import scala.util.control.Breaks._

object StreamUtils {
  def lag(
    input: DataFrame,
    srcCols: Seq[String],
    offsets: Seq[Int],
    dstCols: Seq[String],
    timestampCols: Seq[String],
    thresholds: Seq[Long]
  ): DataFrame = {
    val inputSchema = input.schema
    val colIdxes = srcCols.map(col => inputSchema.fieldIndex(col))
    val timestampColIdxes = timestampCols.map(col => inputSchema.fieldIndex(col))
    val timestampDtypes = (0 until timestampColIdxes.length).map(
      idx => inputSchema.apply(timestampColIdxes(idx)).dataType
    )
    val outputSchema = StructType(
      inputSchema.fields ++ (
        colIdxes.zipWithIndex.map{
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
        val prevTimestampCols = new Array[Array[Array[Long]]](srcCols.length)
        for (i <- 0 until srcCols.length) {
          prevCols(i) = new Array[Any](offsets(i))
          prevTimestampCols(i) = new Array[Array[Long]](offsets(i))
          for (j <- 0 until offsets(i)) {
            prevTimestampCols(i)(j) = Array.fill[Long](
              timestampCols.length
            )(Long.MinValue)
          }
        }
        var rowNum: Int = 0
        val processRows: (Row) => Row = {
          row => {
            val vals = row.toSeq ++ (
              (0 until srcCols.length).map(
                i => {
                  var isRecent = true
                  breakable {
                    for (j <- 0 until timestampCols.length) {
                      val idx = timestampColIdxes(j)
                      val dtype = timestampDtypes(j)
                      val curr = Utils.asLong(Utils.extractValue(row, idx))
                      val prev = prevTimestampCols(i)(rowNum % offsets(i))(j)
                      if (curr - prev > Utils.asLong(thresholds(j))) {
                        isRecent = false
                        break
                      }
                    }
                  }
                  if (isRecent) prevCols(i)(rowNum % offsets(i)) else null
                }
              )
            )

            for (i <- 0 until srcCols.length) {
              prevCols(i)(rowNum % offsets(i)) = row.apply(colIdxes(i))
              for (j <- 0 until timestampCols.length) {
                prevTimestampCols(i)(rowNum % offsets(i))(j) =
                  Utils.asLong(row.apply(timestampColIdxes(j)))
              }
            }

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
