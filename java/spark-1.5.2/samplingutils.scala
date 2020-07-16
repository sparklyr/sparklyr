package sparklyr

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import scala.collection.mutable.WrappedArray
import scala.math.Ordering

object SamplingUtils {
  type Sample = Tuple2[Row, Double]

  // extract k top-priority rows from a RDD efficiently using a bounded priority queue
  def sampleWithoutReplacement(rdd: RDD[Row], priorityColumn: String, k: Int): RDD[Row] = {
    val rows = rdd.top(k)(Ordering.by[Row, Double](r => r.getAs[Double](priorityColumn)))

    val sc = rdd.context
    sc.parallelize(rows)
  }

  // perform weighted sampling with replacement
  // (i.e., scan through all rows only once to choose all k samples)
  def sampleWithReplacement(rdd: RDD[Row], priorityColumn: String, k: Int): RDD[Row] = {
    val sc = rdd.context
    if (0 == k) {
      sc.emptyRDD
    } else {
      val seqOp = (s: Array[Sample], r: Row) => {
        val priorities = r.getAs[Seq[Double]](priorityColumn)

        Range(0, k).foreach(idx => {
          val priority = priorities(idx)
          if (priority > s(idx)._2) s(idx) = (r, priority)
        })

        s
      }

      val combOp = (s1: Array[Sample], s2: Array[Sample]) => {
        Range(0, k).foreach(idx => {
          if (s2(idx)._2 > s1(idx)._2) s1(idx) = s2(idx)
        })

        s1
      }

      val samples = rdd.treeAggregate(
        Array.fill[(Row, Double)](k)((null, Double.NegativeInfinity))
      )(seqOp, combOp, 2)

      sc.parallelize(samples.map(x => x._1))
    }
  }
}
