package sparklyr

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object RddUtils {
  def toRowRDD(rdd: RDD[Double]): RDD[Row] = {
    rdd.map(x => Row(x))
  }
}
