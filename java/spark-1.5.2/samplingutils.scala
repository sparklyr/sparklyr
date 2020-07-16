package sparklyr

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import scala.math.Ordering

object SamplingUtils {
  // extract k top-priority rows from a RDD efficiently using a bounded priority queue
  def top(rdd: RDD[Row], priorityColumn: String, k: Int): RDD[Row] = {
    val sc = rdd.context
    val rows = rdd.top(k)(Ordering.by[Row, Double](r => r.getAs[Double](priorityColumn)))
    sc.parallelize(rows)
  }
}
