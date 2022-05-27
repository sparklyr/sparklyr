package sparklyr

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.NumericType

class WeightedQuantileSummaries(
  // This class implements a weighted version of the Greenwald-Khanna algorithm.
  // The implementation here is structurally similar to
  // https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/QuantileSummaries.scala,
  // the unweighted version of this algorithm.
  val compressThreshold: Double,
  val relativeError: Double,
  val samples: Array[WeightedQuantileSummaries.Stats] = Array.empty,
  val totalWeight: Double = 0.0,
  var compressed: Boolean = false) extends Serializable {

  // buffer of unprocessed samples
  private val buf: ArrayBuffer[(Double, Double)] = ArrayBuffer.empty

  import WeightedQuantileSummaries._

  def insert(value: Double, weight: Double): WeightedQuantileSummaries = {
    buf += ((value, weight))
    compressed = false
    if (buf.size >= defaultBufferSize) {
      val result = this.withBufferedSamplesInserted
      if (result.samples.length >= compressThreshold) {
        result.compress
      } else {
        result
      }
    } else {
      this
    }
  }

  private def withBufferedSamplesInserted: WeightedQuantileSummaries = {
    if (buf.isEmpty) {
      return this
    }
    var currentTotalWeight = totalWeight
    val sorted = buf.toArray.sortWith(_._1 < _._1)
    val newSamples = new ArrayBuffer[Stats]()
    var srcIdx = 0
    var bufIdx = 0
    while (bufIdx < sorted.length) {
      val currentSample = sorted(bufIdx)
      // Add all samples less than or equal to current sample
      while (srcIdx < samples.length && samples(srcIdx).x <= currentSample._1) {
        newSamples += samples(srcIdx)
        srcIdx += 1
      }
      // Add the current sample
      if (srcIdx > 0 && samples(srcIdx - 1).x == currentSample._1) {
        val last = newSamples.last.copy(s = newSamples.last.s + currentSample._2)
        newSamples.dropRight(1)
        newSamples += last
      } else {
        newSamples += Stats(currentSample._1, currentSample._2, 0, 0)
      }
      currentTotalWeight += currentSample._2
      bufIdx += 1
    }
    // Add all remaining existing samples
    while (srcIdx < samples.length) {
      newSamples += samples(srcIdx)
      srcIdx += 1
    }
    new WeightedQuantileSummaries(
      compressThreshold = compressThreshold,
      relativeError = relativeError,
      samples = newSamples.toArray,
      totalWeight = currentTotalWeight
    )
  }

  def compress(): WeightedQuantileSummaries = {
    val result = this.withBufferedSamplesInserted
    assert(result.buf.isEmpty)
    val compressed = compressImmut(
      result.samples,
      totalWeight = result.totalWeight,
      mergeThreshold = relativeError * result.totalWeight
    )
    new WeightedQuantileSummaries(
      compressThreshold = compressThreshold,
      relativeError = relativeError,
      samples = compressed,
      totalWeight = result.totalWeight,
      compressed = true
    )
  }

  private def shallowCopy: WeightedQuantileSummaries = {
    new WeightedQuantileSummaries(
      compressThreshold = compressThreshold,
      relativeError = relativeError,
      samples = samples,
      totalWeight = totalWeight,
      compressed = compressed
    )
  }

  def merge(other: WeightedQuantileSummaries): WeightedQuantileSummaries = {
    require(buf.isEmpty, "Current buffer needs to be compressed before merge")
    require(
      other.buf.isEmpty, "Other buffer needs to be compressed before merge"
    )
    if (other.samples.isEmpty) {
      this.shallowCopy
    } else if (samples.isEmpty) {
      other.shallowCopy
    } else {
      val mergedSamples = new ArrayBuffer[Stats]()
      val mergedRelativeError = math.max(relativeError, other.relativeError)
      val mergedTotalWeight = totalWeight + other.totalWeight

      var selfIdx = 0
      var otherIdx = 0

      while (selfIdx < samples.length && otherIdx < other.samples.length) {
        val selfSample = samples(selfIdx)
        val otherSample = other.samples(otherIdx)

        val nextSample = (
          if (selfSample.x < otherSample.x) {
            selfIdx += 1
            selfSample
          } else {
            otherIdx += 1
            otherSample
          }
        )

        if (!mergedSamples.isEmpty && nextSample.x == mergedSamples.last.x) {
          val last = mergedSamples.last.copy(
            s = mergedSamples.last.s + nextSample.s,
            l = mergedSamples.last.l + nextSample.l,
            r = mergedSamples.last.r + nextSample.r
          )
          mergedSamples.dropRight(1)
          mergedSamples += last
        } else {
          mergedSamples += nextSample
        }
      }

      while (selfIdx < samples.length) {
        mergedSamples += samples(selfIdx)
        selfIdx += 1
      }
      while (otherIdx < other.samples.length) {
        mergedSamples += other.samples(otherIdx)
        otherIdx += 1
      }

      val compressed = compressImmut(
        mergedSamples.toArray,
        totalWeight = mergedTotalWeight,
        mergeThreshold = mergedRelativeError * mergedTotalWeight
      )
      new WeightedQuantileSummaries(
        other.compressThreshold,
        mergedRelativeError,
        compressed,
        mergedTotalWeight,
        true
      )
    }
  }

  def query(quantile: Double): Option[Double] = {
    require(
      quantile >= 0 && quantile <= 1.0,
      "quantile should be in the range [0.0, 1.0]"
    )
    require(
      buf.isEmpty,
      "Cannot operate on an uncompressed summary, call compress() first"
    )

    if (samples.isEmpty) return None

    if (quantile <= relativeError) {
      return Some(samples.head.x)
    }

    if (quantile >= 1 - relativeError) {
      return Some(samples.last.x)
    }

    val targetRank = quantile * totalWeight
    val tol = relativeError * totalWeight

    var i: Int = 0
    var minRank: Double = 0
    var rightSum: Double = samples.map(sample => sample.s + sample.r).sum
    while (i < samples.length - 1) {
      val currentSample = samples(i)
      minRank += currentSample.l
      rightSum -= currentSample.s
      val maxRank = totalWeight - rightSum
      if (targetRank - tol <= minRank + currentSample.s &&
          maxRank - currentSample.s <= targetRank + tol) {
        return Some(currentSample.x)
      }
      minRank += currentSample.s
      rightSum -= currentSample.r
      i += 1
    }
    Some(samples.last.x)
  }
}

object WeightedQuantileSummaries {
  val defaultCompressThreshold: Int = 10000
  val defaultBufferSize: Int = 50000
  val defaultRelativeError: Double = 0.01

  case class Stats(x: Double, s: Double, l: Double, r: Double)

  private def compressImmut(
    currentSamples: Array[Stats],
    totalWeight: Double,
    mergeThreshold: Double
  ): Array[Stats] = {
    if (currentSamples.length < 3) {
      return currentSamples
    }
    val res = ListBuffer.empty[Stats]
    var head = currentSamples.last
    var i = currentSamples.length - 2
    var leftSum: Double = (0 until i - 1).map(
      j => currentSamples(j).s + currentSamples(j).l
    ).sum +
      currentSamples(i - 1).l
    var rightSum: Double = head.r
    var leftIncrement: Double = 0
    var rightIncrement: Double = 0
    var headRightSum = rightSum
    // Do not compress the last element
    while (i >= 1) {
      val gap = totalWeight	- headRightSum - leftSum
      leftSum -= currentSamples(i - 1).l
      if (i > 1) {
        leftSum -= currentSamples(i - 2).s
      }
      rightSum += currentSamples(i + 1).s + currentSamples(i).r
      if (gap >= mergeThreshold) {
        // delete everything between the (i + 1)-th sample and head
        head = head.copy(l = head.l + leftIncrement)
        currentSamples(i) = currentSamples(i).copy(r = currentSamples(i).r + rightIncrement)
        res.prepend(head)
        head = currentSamples(i)
        headRightSum = rightSum
        leftIncrement = 0
        rightIncrement = 0
      } else {
        leftIncrement += currentSamples(i).l + currentSamples(i).s
        rightIncrement += currentSamples(i).r + currentSamples(i).s
      }
      i -= 1
    }
    res.prepend(head)
    if (currentSamples.length > 1) {
      res.prepend(currentSamples.head)
    }
    res.toArray
  }

  def approxWeightedQuantile(
    df: DataFrame,
    cols: Array[String],
    weightCol: String,
    probabilities: Array[Double],
    relativeError: Double
  ): Array[Array[Double]] = {
    require(relativeError >= 0,
      s"Relative Error must be non-negative but got $relativeError")
    val requiredCols = cols ++ Array(weightCol)
    val colExprs: Seq[Column] = requiredCols.map(
      colName => {
        new Column(colName).cast(DoubleType)
      }
    )
    val emptySummaries = Array.fill(cols.size)(
      new WeightedQuantileSummaries(
        WeightedQuantileSummaries.defaultCompressThreshold, relativeError
      )
    )

    def apply(
      summaries: Array[WeightedQuantileSummaries],
      row: Row
    ): Array[WeightedQuantileSummaries] = {
      var i = 0
      while (i < summaries.length) {
        if (!row.isNullAt(i)) {
          val v = row.getDouble(i)
          if (!v.isNaN) {
            val w = row.getDouble(row.fieldIndex(weightCol))
            summaries(i) = summaries(i).insert(v, w)
          }
        }
        i += 1
      }
      summaries
    }

    def merge(
      sum1: Array[WeightedQuantileSummaries],
      sum2: Array[WeightedQuantileSummaries]
    ): Array[WeightedQuantileSummaries] = {
      sum1.zip(sum2).map { case (s1, s2) => s1.compress.merge(s2.compress) }
    }

    val summaries = df
      .select((requiredCols).map(col): _*)
      .select(colExprs: _*)
      .rdd
      .treeAggregate(emptySummaries)(apply, merge)

    summaries.map { summary => probabilities.flatMap(summary.query) }
  }
}
