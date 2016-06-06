import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object utils {

  def plus2(base:Int) : Int = {
    return base + 2
  }

  // See: http://spark.apache.org/docs/latest/mllib-clustering.html
  def schemaRddToVectorRdd(schemaRdd:RDD[Row]) : RDD[Vector] = {
    schemaRdd.map(row => {
      Vectors.dense(row.toSeq.toArray.map({
        case s: String => s.toDouble
        case l: Long => l.toDouble
        case i: Integer => i.toDouble
        case d: Double => d
        case _ => 0.0
      }))
    })
  }
}
