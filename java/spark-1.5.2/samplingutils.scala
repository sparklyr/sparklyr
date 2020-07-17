package sparklyr

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import scala.util.Random

object SamplingUtils {
  case class Sample(val priority: Double, val row: Row) extends Ordered[Sample] {
    override def compare(that: Sample): Int = {
      scala.math.signum(priority - that.priority).toInt
    }
  }

  def sampleWithoutReplacement(
    rdd: RDD[Row],
    weightColumn: String,
    k: Int,
    random: Random
  ): RDD[Row] = {
    val sc = rdd.context
    if (0 == k) {
      sc.emptyRDD
    } else {
      val mapRDDs = rdd.mapPartitions { iter =>
        val pq = new BoundedPriorityQueue[Sample](k)

        for (row <- iter) {
          val weight = row.getAs[Double](weightColumn)
          if (weight > 0) {
            val sample = Sample(scala.math.log(random.nextDouble) / weight, row)
            pq += sample
          }
        }

        Iterator.single(pq)
      }

      if (0 == mapRDDs.partitions.length) {
        sc.emptyRDD
      } else {
        sc.parallelize(
          mapRDDs.reduce(
            (pq1, pq2) => {
              pq1 ++= pq2

              pq1
            }
          ).toSeq.map(x => x.row)
        )
      }
    }
  }

  def sampleWithReplacement(
    rdd: RDD[Row],
    weightColumn: String,
    k: Int,
    random: Random
  ): RDD[Row] = {
    val sc = rdd.context
    if (0 == k) {
      sc.emptyRDD
    } else {
      val seqOp = (s: Array[Sample], r: Row) => {
        val weight = r.getAs[Double](weightColumn)

        if (weight > 0)
          Range(0, k).par.foreach(idx => {
            val priority = scala.math.log(random.nextDouble) / weight
            if (s(idx).priority < priority) s(idx) = Sample(priority, r)
          })

        s
      }
      val combOp = (s1: Array[Sample], s2: Array[Sample]) => {
        Range(0, k).par.foreach(idx => {
          if (s1(idx).priority < s2(idx).priority)
            s1(idx) = s2(idx)
        })

        s1
      }

      sc.parallelize(
        rdd.treeAggregate(
          Array.fill[Sample](k)(Sample(Double.NegativeInfinity, null))
        )
        (seqOp, combOp, 2)
        .map(x => x.row)
      )
    }
  }
}
