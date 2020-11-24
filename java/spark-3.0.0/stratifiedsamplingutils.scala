package sparklyr

import java.util.HashMap
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

import scala.collection.JavaConverters._
import scala.util.Random

object StratifiedSamplingUtils {
  type Sample = SamplingUtils.Sample
  type SamplesPQ = BoundedPriorityQueue[Sample]
  type SamplesPQMap = HashMap[scala.collection.immutable.List[Any], BoundedPriorityQueue[Sample]]
  type SamplesArray = Array[Sample]
  type SamplesArrayMap = HashMap[scala.collection.immutable.List[Any], Array[Sample]]
  type StrataSampleSizeMap = scala.collection.immutable.Map[scala.collection.immutable.List[Any], Int]

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

  def sampleFracWithoutReplacement(
    df: DataFrame,
    groupVars: Seq[String],
    weightColumn: String,
    frac: Double,
    seed: Long
  ): RDD[Row] = {
    val schema = df.schema
    val groups = df.groupBy(groupVars.map(x => new Column(x)): _*)
    val groupCols = Set(groupVars: _*)
    val sampleAgg = sampleFracWithoutReplacementAggregator(
      df = df,
      groupVars = groupVars,
      weightColumn = weightColumn,
      frac = frac,
      seed = seed
    )

    val rows = groups
      .as(RowEncoder(schema.apply(groupCols)), RowEncoder(schema))
      .agg(sampleAgg.toColumn)
      .collect
      .flatMap(
        x =>
          x._2.values.asScala.flatMap(
            e => e.toArray.flatMap(s => Iterator(s.row))
          )
      )

    df.rdd.context.parallelize(rows)
  }

  def sampleFracWithReplacement(
    df: DataFrame,
    groupVars: Seq[String],
    weightColumn: String,
    frac: Double,
    seed: Long
  ): RDD[Row] = {
    val schema = df.schema
    val groups = df.groupBy(groupVars.map(x => new Column(x)): _*)
    val groupCols = Set(groupVars: _*)
    val sampleAgg = sampleFracWithReplacementAggregator(
      df = df,
      groupVars = groupVars,
      weightColumn = weightColumn,
      frac = frac,
      seed = seed
    )

    val rows = groups
      .as(RowEncoder(schema.apply(groupCols)), RowEncoder(schema))
      .agg(sampleAgg.toColumn)
      .collect
      .flatMap(
        x =>
          x._2.values.asScala.flatMap(
            e => e.flatMap(s => Iterator(s.row))
          )
      )

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
        var weight = SamplingUtils.extractWeightValue(a, weightColumn)
        if (weight > 0) {
          val sampleSeed = seed +
            groupVars
              .map(c => { a.get(a.fieldIndex(c)).hashCode })
              .sum
          val random = prngState.computeIfAbsent(
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

      private[this] val prngState = new ConcurrentHashMap[Long, Random]
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
        var weight = SamplingUtils.extractWeightValue(a, weightColumn)
        if (weight > 0) {
          val sampleSeed = seed +
            groupVars
              .map(c => { a.get(a.fieldIndex(c)).hashCode })
              .sum
          val random = prngState.computeIfAbsent(
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

      private[this] val prngState = new ConcurrentHashMap[Long, Random]
    }
  }

  private[this] def sampleFracWithoutReplacementAggregator(
    df: DataFrame,
    groupVars: Seq[String],
    weightColumn: String,
    frac: Double,
    seed: Long
  ) = {
    val sampleSizes = computeStrataSampleSizes(df, groupVars, frac)

    new Aggregator[Row, SamplesPQMap, SamplesPQMap] {
      override def zero: SamplesPQMap = new HashMap

      override def reduce(b: SamplesPQMap, a: Row): SamplesPQMap = {
        var weight = SamplingUtils.extractWeightValue(a, weightColumn)
        if (weight > 0) {
          val sampleSeed = seed +
            groupVars
              .map(c => { a.get(a.fieldIndex(c)).hashCode })
              .sum
          val random = prngState.computeIfAbsent(
            sampleSeed,
            x => new Random(x)
          )
          val sample = SamplingUtils.Sample(
            SamplingUtils.genSamplePriority(weight, random), a
          )

          updateSamplesPQ(
            sample = sample,
            samplesPQMap = b,
            groupVars = groupVars,
            sampleSizes = sampleSizes
          )
        }

        b
      }

      override def merge(b1: SamplesPQMap, b2: SamplesPQMap): SamplesPQMap = {
        b2.entrySet.stream.forEach(
          entry => {
            val pq = b1.computeIfAbsent(
              entry.getKey,
              x => new SamplesPQ(sampleSizes.get(x).get)
            )

            pq ++= entry.getValue
          }
        )

        b1
      }

      override def finish(reduction: SamplesPQMap): SamplesPQMap = {
        reduction
      }

      override def bufferEncoder(): Encoder[SamplesPQMap] = {
        Encoders.kryo[SamplesPQMap]
      }

      override def outputEncoder(): Encoder[SamplesPQMap] = {
        Encoders.kryo[SamplesPQMap]
      }

      private[this] def updateSamplesPQ(
        sample: Sample,
        samplesPQMap: SamplesPQMap,
        groupVars: Seq[String],
        sampleSizes: StrataSampleSizeMap
      ): Unit = {
        val pq = samplesPQMap.computeIfAbsent(
          extractGroupKey(sample.row, groupVars),
          x => new SamplesPQ(sampleSizes.get(x).get)
        )
        pq += sample
      }

      private[this] val prngState = new ConcurrentHashMap[Long, Random]
    }
  }

  private[this] def sampleFracWithReplacementAggregator(
    df: DataFrame,
    groupVars: Seq[String],
    weightColumn: String,
    frac: Double,
    seed: Long
  ) = {
    val sampleSizes = computeStrataSampleSizes(df, groupVars, frac)

    new Aggregator[Row, SamplesArrayMap, SamplesArrayMap] {
      override def zero: SamplesArrayMap = new HashMap

      override def reduce(b: SamplesArrayMap, a: Row): SamplesArrayMap = {
        var weight = SamplingUtils.extractWeightValue(a, weightColumn)
        if (weight > 0) {
          val sampleSeed = seed +
            groupVars
              .map(c => { a.get(a.fieldIndex(c)).hashCode })
              .sum
          val random = prngState.computeIfAbsent(
            sampleSeed,
            x => new Random(x)
          )

          updateSamplesArray(
            random = random,
            row = a,
            weight = weight,
            samplesArrayMap = b,
            groupVars = groupVars,
            sampleSizes = sampleSizes
          )
        }

        b
      }

      override def merge(b1: SamplesArrayMap, b2: SamplesArrayMap): SamplesArrayMap = {
        b2.entrySet.stream.forEach(
          entry => {
            val sampleSize = sampleSizes.get(entry.getKey).get
            val samples = b1.computeIfAbsent(
              entry.getKey,
              x =>
                Array.fill[Sample](sampleSize)(
                  SamplingUtils.Sample(Double.NegativeInfinity, null)
                )
            )
            val replacements = entry.getValue

            (0 until sampleSize).foreach(
              idx => {
                if (samples(idx) < replacements(idx)) {
                  samples(idx) = replacements(idx)
                }
              }
            )
          }
        )

        b1
      }

      override def finish(reduction: SamplesArrayMap): SamplesArrayMap = {
        reduction
      }

      override def bufferEncoder(): Encoder[SamplesArrayMap] = {
        Encoders.kryo[SamplesArrayMap]
      }

      override def outputEncoder(): Encoder[SamplesArrayMap] = {
        Encoders.kryo[SamplesArrayMap]
      }

      private[this] def updateSamplesArray(
        random: Random,
        row: Row,
        weight: Double,
        samplesArrayMap: SamplesArrayMap,
        groupVars: Seq[String],
        sampleSizes: StrataSampleSizeMap
      ): Unit = {
        val key = extractGroupKey(row, groupVars)
        val sampleSize = sampleSizes.get(key).get
        val samples = samplesArrayMap.computeIfAbsent(
          key,
          x =>
            Array.fill[Sample](sampleSize)(
              SamplingUtils.Sample(Double.NegativeInfinity, null)
            )
        )

        Range(0, sampleSize).map(
          idx => {
            val replacement = SamplingUtils.Sample(
              SamplingUtils.genSamplePriority(weight, random), row
            )
            if (samples(idx) < replacement) {
              samples(idx) = replacement
            }
          }
        )
      }

      private[this] val prngState = new ConcurrentHashMap[Long, Random]
    }
  }

  private[this] def extractGroupKey(
    row: Row,
    groupVars: Seq[String]
  ): scala.collection.immutable.List[Any] = {
    groupVars
      .map(c => { row.get(row.fieldIndex(c)) })
      .toList
  }

  private[this] def computeStrataSampleSizes(
    df: DataFrame,
    groupVars: Seq[String],
    frac: Double
  ): StrataSampleSizeMap = {
    df.groupBy(groupVars.map(x => new Column(x)): _*)
      .count
      .collect
      .map(row => {
        val key = extractGroupKey(row, groupVars)
        val sampleSize = Math.ceil(row.getLong(groupVars.length) * frac).intValue

        (key, sampleSize)
      })
      .toMap
  }
}
