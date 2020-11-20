package sparklyr

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters;
import scala.util.Random

object StratifiedSamplingUtils {
  type Sample = SamplingUtils.Sample
  type SamplesPQ = BoundedPriorityQueue[Sample]
  type SamplesArray = Array[Sample]

  def sampleWithoutReplacement(
    df: DataFrame,
    groupVars: Seq[String],
    weightColumn: String,
    k: Int,
    seed: Long
  ): RDD[Row] = {
    val schema = df.schema
    val groups = df.groupBy(groupVars.map(x => new Column(x)): _*)
    val groupCols = Set(groupVars: _*)
    val sampleAgg = sampleWithoutReplacementAggregator(
      groupVars = groupVars,
      weightColumn = weightColumn,
      k = k,
      seed = seed
    )

    val rows = groups
      .as(RowEncoder(schema.apply(groupCols)), RowEncoder(schema))
      .agg(sampleAgg.toColumn)
      .collect
      .flatMap(x => x._2.toSeq.map(s => s.row))

    df.rdd.context.parallelize(rows)
  }

  def sampleWithReplacement(
    df: DataFrame,
    groupVars: Seq[String],
    weightColumn: String,
    k: Int,
    seed: Long
  ): RDD[Row] = {
    val schema = df.schema
    val groups = df.groupBy(groupVars.map(x => new Column(x)): _*)
    val groupCols = Set(groupVars: _*)
    val sampleAgg = sampleWithReplacementAggregator(
      groupVars = groupVars,
      weightColumn = weightColumn,
      k = k,
      seed = seed
    )

    val rows = groups
      .as(RowEncoder(schema.apply(groupCols)), RowEncoder(schema))
      .agg(sampleAgg.toColumn)
      .collect
      .flatMap(x => x._2.map(s => s.row))

    df.rdd.context.parallelize(rows)
  }

  private[this] def sampleWithoutReplacementAggregator(
    groupVars: Seq[String],
    weightColumn: String,
    k: Int,
    seed: Long
  ) = {
    new Aggregator[Row, SamplesPQ, SamplesPQ] {
      override def zero: SamplesPQ = new SamplesPQ(k)

      override def reduce(b: SamplesPQ, a: Row): SamplesPQ = {
        val sampleSeed = seed +
          groupVars
            .map(c => { a.get(a.fieldIndex(c)).hashCode })
            .sum
        var weight = SamplingUtils.extractWeightValue(a, weightColumn)
        if (weight > 0) {
          val random = prngMap.computeIfAbsent(
            sampleSeed,
            x => new Random(x)
          )
          val sample = SamplingUtils.Sample(
            SamplingUtils.genSamplePriority(weight, random), a
          )
          b += sample
        }

        b
      }

      override def merge(b1: SamplesPQ, b2: SamplesPQ): SamplesPQ = {
        b1 ++= b2

        b1
      }

      override def finish(reduction: SamplesPQ): SamplesPQ = {
        reduction
      }

      override def bufferEncoder(): Encoder[SamplesPQ] = {
        Encoders.kryo[SamplesPQ]
      }

      override def outputEncoder(): Encoder[SamplesPQ] = {
        Encoders.kryo[SamplesPQ]
      }

      private[this] val prngMap = new ConcurrentHashMap[Long, Random]
    }
  }

  private[this] def sampleWithReplacementAggregator(
    groupVars: Seq[String],
    weightColumn: String,
    k: Int,
    seed: Long
  ) = {
    new Aggregator[Row, SamplesArray, SamplesArray] {
      override def zero: SamplesArray = Array.fill[Sample](k)(
        SamplingUtils.Sample(Double.NegativeInfinity, null)
      )

      override def reduce(b: SamplesArray, a: Row): SamplesArray = {
        val sampleSeed = seed +
          groupVars
            .map(c => { a.get(a.fieldIndex(c)).hashCode })
            .sum
        var weight = SamplingUtils.extractWeightValue(a, weightColumn)
        if (weight > 0) {
          val random = prngMap.computeIfAbsent(
            sampleSeed,
            x => new Random(x)
          )
          Range(0, k).foreach(
            idx => {
              val replacement = SamplingUtils.Sample(
                SamplingUtils.genSamplePriority(weight, random), a
              )
              if (b(idx) < replacement) {
                b(idx) = replacement
              }
            }
          )
        }

        b
      }

      override def merge(b1: SamplesArray, b2: SamplesArray): SamplesArray = {
        Range(0, k).foreach(idx => {
          if (b1(idx) < b2(idx)) {
            b1(idx) = b2(idx)
          }
        })

        b1
      }

      override def finish(reduction: SamplesArray): SamplesArray = {
        reduction
      }

      override def bufferEncoder(): Encoder[SamplesArray] = {
        Encoders.kryo[SamplesArray]
      }

      override def outputEncoder(): Encoder[SamplesArray] = {
        Encoders.kryo[SamplesArray]
      }

      private[this] val prngMap = new ConcurrentHashMap[Long, Random]
    }
  }
}
