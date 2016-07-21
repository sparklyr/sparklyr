package sparklyr

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.r.SerDe
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression, GenericRowWithSchema}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object Translator {
  def dfToCols(df: DataFrame): Array[Array[Any]] = {
    val localDF: Array[Row] = df.collect()
    val numCols = df.columns.length
    val numRows = localDF.length

    val colArray = new Array[Array[Any]](numCols)
    for (colNo <- 0 until numCols) {
      colArray(colNo) = new Array[Any](numRows)
      for (rowNo <- 0 until numRows) {
        colArray(colNo)(rowNo) = localDF(rowNo)(colNo)
      }
    }
    colArray
  }
}
