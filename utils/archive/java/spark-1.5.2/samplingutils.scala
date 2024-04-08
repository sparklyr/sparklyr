package sparklyr

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.util.Random

object SamplingUtils {
  type SamplesPQ = BoundedPriorityQueue[Sample]

  private[sparklyr] case class Sample(val priority: Double, val row: Row) extends Ordered[Sample] {
    override def compare(that: Sample): Int = {
      scala.math.signum(priority - that.priority).toInt
    }
  }

  private[this] case class PRNG() extends java.util.function.Function[Long, Random] {
    override def apply(x: Long): Random = new Random(x)
  }

  def sampleWithoutReplacement(
    rdd: RDD[Row],
    weightColumn: String,
    k: Int,
    seed: Long
  ): RDD[Row] = {
    val sc = rdd.context
    if (0 == k) {
      sc.emptyRDD
    } else {
      val prngState = new ConcurrentHashMap[Long, Random]
      val samples = rdd.aggregate(
        zeroValue = new SamplesPQ(k)
      )(
        seqOp = (pq: SamplesPQ, row: Row) => {
          var weight = extractWeightValue(row, weightColumn)
          if (weight > 0) {
            val sampleSeed = seed + TaskContext.getPartitionId
            val random = prngState.computeIfAbsent(
              sampleSeed,
              new PRNG
            )
            val sample = Sample(genSamplePriority(weight, random), row)
            pq += sample
          }

          pq
        },
        combOp = (pq1: SamplesPQ, pq2: SamplesPQ) => {
          pq1 ++= pq2

          pq1
        }
      )

      sc.parallelize(samples.toSeq.map(x => x.row))
    }
  }

  def sampleWithReplacement(
    rdd: RDD[Row],
    weightColumn: String,
    k: Int,
    seed: Long
  ): RDD[Row] = {
    val sc = rdd.context
    if (0 == k) {
      sc.emptyRDD
    } else {
      val mapRDDs = rdd.mapPartitionsWithIndex { (index, iter) =>
        val random = new Random(seed + index)
        val samples = Array.fill[Sample](k)(
          Sample(Double.NegativeInfinity, null)
        )
        for (row <- iter) {
          var weight = extractWeightValue(row, weightColumn)
          if (weight > 0)
            Range(0, k).foreach(idx => {
              val replacement = Sample(genSamplePriority(weight, random), row)
              if (samples(idx) < replacement)
                samples(idx) = replacement
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
                if (s1(idx) < s2(idx)) s1(idx) = s2(idx)
              })

              s1
            }
          ).map(x => x.row)
        )
      }
    }
  }

  // generate a sampling priority for a row given the sampling weight and
  // source of randomness
  private[sparklyr] def genSamplePriority(weight: Double, random: Random): Double = {
    scala.math.log(random.nextDouble) / weight
  }

  private[sparklyr] def extractWeightValue(row: Row, weightColumn: String): Double = {
    if (null == weightColumn || weightColumn.isEmpty) {
      1.0
    } else {
      Utils.asDouble(row.get(row.fieldIndex(weightColumn)))
    }
  }
}
