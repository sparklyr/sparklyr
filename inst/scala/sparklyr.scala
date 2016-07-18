import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import scala.util.Try

object utils {

  def readColumnInt(rdd: RDD[Row]): Array[Int] = {
    rdd.map(row => row(0).asInstanceOf[Int]).collect()
  }

  def readColumnDouble(rdd: RDD[Row]): Array[Double] = {
    rdd.map(row => row(0).asInstanceOf[Double]).collect()
  }

  def readColumnBoolean(rdd: RDD[Row]): Array[Boolean] = {
    rdd.map(row => row(0).asInstanceOf[Boolean]).collect()
  }

  def readColumnString(rdd: RDD[Row]): String = {
    val column = rdd.map(row => row(0).asInstanceOf[String]).collect()
    val escaped = column.map(string => StringEscapeUtils.escapeCsv(string))
    val joined = escaped.mkString("\n")
    return joined + "\n"
  }

  def readColumnDefault(rdd: RDD[Row]): Array[Any] = {
    rdd.map(row => row(0)).collect()
  }

  def createDataFrame(sc: SparkContext, rows: Array[_], partitions: Int): RDD[Row] = {
    var data = rows.map(o => {
      val r = o.asInstanceOf[Array[_]]
      org.apache.spark.sql.Row.fromSeq(r)
    })

    sc.parallelize(data, partitions)
  }

  def createDataFrameFromText(
    sc: SparkContext,
    rows: Array[String],
    columns: Array[String],
    partitions: Int): RDD[Row] = {

    var data = rows.map(o => {
      val r = o.split('|')
      var typed = (Array.range(0, r.length)).map(idx => {
        val column = columns(idx)
        val value = r(idx)

        column match {
          case "integer"  => if (Try(value.toInt).isSuccess) value.toInt else null.asInstanceOf[Int]
          case "double"  => if (Try(value.toDouble).isSuccess) value.toDouble else null.asInstanceOf[Double]
          case "logical" => if (Try(value.toBoolean).isSuccess) value.toBoolean else null.asInstanceOf[Boolean]
          case _ => value
        }
      })

      org.apache.spark.sql.Row.fromSeq(typed)
    })

    sc.parallelize(data, partitions)
  }

  def classExists(name: String): Boolean = {
    scala.util.Try(Class.forName(name)).isSuccess
  }
}
