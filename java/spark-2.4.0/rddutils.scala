package sparklyr

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object RddUtils {
  def doubleToRow(rdd: RDD[Double]): RDD[Row] = {
    rdd.map(x => Row(x))
  }

  def integerToRow(rdd: RDD[Int]): RDD[Row] = {
    rdd.map(x => Row(x))
  }
}
