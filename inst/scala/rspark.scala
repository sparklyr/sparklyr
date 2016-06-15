import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object utils {

  def castColumnDouble(df: DataFrame, inputColName: String, outputColName: String): DataFrame = {
    df.withColumn(outputColName, df.col(inputColName).cast(DataTypes.DoubleType))
  }

  def castColumn(df: DataFrame, inputColName: String, outputColName: String, outputType: String): DataFrame = {
    outputType match {
      case "DoubleType" => castColumnDouble(df, inputColName, outputColName)
      case _ => throw new IllegalArgumentException(s"Casting to type '${outputType}' not yet implemented")
    }
  }

  def readColumnInt(df: DataFrame, colName: String): Array[Int] = {
    df.select(colName).rdd.map(row => row(0).asInstanceOf[Int]).collect()
  }

  def readColumnDouble(df: DataFrame, colName: String): Array[Double] = {
    df.select(colName).rdd.map(row => row(0).asInstanceOf[Double]).collect()
  }

  def readColumnBoolean(df: DataFrame, colName: String): Array[Boolean] = {
    df.select(colName).rdd.map(row => row(0).asInstanceOf[Boolean]).collect()
  }

  def readColumnString(df: DataFrame, colName: String): String = {
    val column = df.select(colName).rdd.map(row => row(0).asInstanceOf[String]).collect()
    val escaped = column.map(string => StringEscapeUtils.escapeCsv(string))
    val joined = escaped.mkString("\n")
    return joined + "\n"
  }

  def readColumnDefault(df: DataFrame, colName: String): Array[Any] = {
    df.select(colName).rdd.map(row => row(0)).collect()
  }
}
