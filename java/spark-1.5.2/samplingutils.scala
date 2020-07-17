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
      val maxWeight = getMaxWeight(rdd, weightColumn)
      val mapRDDs = rdd.mapPartitions { iter =>
        val pq = new BoundedPriorityQueue[Sample](k)

        for (row <- iter) {
          var weight = row.getAs[Double](weightColumn)
          if (weight > 0) {
            if (maxWeight > 0) weight /= maxWeight
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
      val maxWeight = getMaxWeight(rdd, weightColumn)
      val mapRDDs = rdd.mapPartitions { iter =>
        val samples = Array.fill[Sample](k)(Sample(Double.NegativeInfinity, null))
        for (row <- iter) {
          var weight = row.getAs[Double](weightColumn)
          if (weight > 0)
            if (maxWeight > 0) weight /= maxWeight
            Range(0, k).foreach(idx => {
              val priority = scala.math.log(random.nextDouble) / weight
              if (samples(idx).priority < priority)
                samples(idx) = Sample(priority, row)
            })
        }

        Iterator.single(samples)
      }

      if (0 == mapRDDs.partitions.length) {
        sc.emptyRDD
      } else {
        sc.parallelize(
          mapRDDs.reduce(
            (s1, s2) => {
              Range(0, k).foreach(idx => {
                if (s1(idx).priority < s2(idx).priority) s1(idx) = s2(idx)
              })

              s1
            }
          ).map(x => x.row)
        )
      }
    }
  }

  def getMaxWeight(rdd: RDD[Row], weightColumn: String) : Double = {
    if (0 == rdd.count) {
      1
    } else {
      rdd.mapPartitions(
        iter => {
          iter.map(_.getAs[Double](weightColumn))
        }
      ).max
    }
  }
}
