// TODO: possibly make this along with `WeightedQuantileSummaries` part of
//       Apache Spark as a generalization of the `QuantileDiscretizer`
//       (this file is partially copy-pasted from QuantileDiscretizer.scala)
package org.apache.spark.ml.feature

import org.apache.spark.internal.Logging
import org.apache.spark.ml._
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType

private[feature] trait QuantileDiscretizerBase extends Params
  with HasHandleInvalid with HasInputCol with HasOutputCol with HasInputCols with HasOutputCols {
  val numBuckets = new IntParam(this, "numBuckets", "Number of buckets (quantiles, or " +
    "categories) into which data points are grouped. Must be >= 2.",
    ParamValidators.gtEq(2))

  def getNumBuckets: Int = getOrDefault(numBuckets)

  val numBucketsArray = new IntArrayParam(this, "numBucketsArray", "Array of number of buckets " +
    "(quantiles, or categories) into which data points are grouped. This is for multiple " +
    "columns input. If transforming multiple columns and numBucketsArray is not set, but " +
    "numBuckets is set, then numBuckets will be applied across all columns.",
    (arrayOfNumBuckets: Array[Int]) => arrayOfNumBuckets.forall(ParamValidators.gtEq(2)))

  def getNumBucketsArray: Array[Int] = $(numBucketsArray)

  override val handleInvalid: Param[String] = new Param[String](this, "handleInvalid",
    "how to handle invalid entries. Options are skip (filter out rows with invalid values), " +
    "error (throw an error), or keep (keep invalid values in a special additional bucket).",
    ParamValidators.inArray(Bucketizer.supportedHandleInvalids))

  setDefault(handleInvalid -> Bucketizer.ERROR_INVALID, numBuckets -> 2)
}

final class WeightedQuantileDiscretizer(override val uid: String)
  extends Estimator[Bucketizer] with QuantileDiscretizerBase with HasWeightCol with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("weightedQuantileDiscretizer"))

  final val relativeError: DoubleParam = new DoubleParam(this, "relativeError", "the relative target precision for the approximate quantile algorithm. Must be in the range [0, 1]", ParamValidators.inRange(0, 1))
  setDefault(relativeError, 0.001)
  final def getRelativeError: Double = $(relativeError)

  def setRelativeError(value: Double): this.type = set(relativeError, value)

  def setNumBuckets(value: Int): this.type = set(numBuckets, value)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  def setNumBucketsArray(value: Array[Int]): this.type = set(numBucketsArray, value)

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  override def transformSchema(schema: StructType): StructType = {
    ParamValidators.checkSingleVsMultiColumnParams(this, Seq(outputCol),
      Seq(outputCols))

    if (isSet(inputCol)) {
      require(!isSet(numBucketsArray),
        s"numBucketsArray can't be set for single-column WeightedQuantileDiscretizer.")
    }

    if (isSet(inputCols)) {
      require(getInputCols.length == getOutputCols.length,
        s"WeightedQuantileDiscretizer $this has mismatched Params " +
          s"for multi-column transform. Params (inputCols, outputCols) should have " +
          s"equal lengths, but they have different lengths: " +
          s"(${getInputCols.length}, ${getOutputCols.length}).")
      if (isSet(numBucketsArray)) {
        require(getInputCols.length == getNumBucketsArray.length,
          s"WeightedQuantileDiscretizer $this has mismatched Params " +
            s"for multi-column transform. Params (inputCols, outputCols, numBucketsArray) " +
            s"should have equal lengths, but they have different lengths: " +
            s"(${getInputCols.length}, ${getOutputCols.length}, ${getNumBucketsArray.length}).")
        require(!isSet(numBuckets),
          s"exactly one of numBuckets, numBucketsArray Params to be set, but both are set." )
      }
    }

    val (inputColNames, outputColNames) = if (isSet(inputCols)) {
      ($(inputCols).toSeq, $(outputCols).toSeq)
    } else {
      (Seq($(inputCol)), Seq($(outputCol)))
    }

    var outputFields = schema.fields
    inputColNames.zip(outputColNames).foreach { case (inputColName, outputColName) =>
      SchemaUtils.checkNumericType(schema, inputColName)
      require(!schema.fieldNames.contains(outputColName),
        s"Output column $outputColName already exists.")
      val attr = NominalAttribute.defaultAttr.withName(outputColName)
      outputFields :+= attr.toStructField()
    }
    StructType(outputFields)
  }

  override def fit(dataset: Dataset[_]): Bucketizer = {
    transformSchema(dataset.schema, logging = true)
    val bucketizer = new Bucketizer(uid).setHandleInvalid($(handleInvalid))
    if (isSet(inputCols)) {
      val splitsArray = if (isSet(numBucketsArray)) {
        val probArrayPerCol = $(numBucketsArray).map { numOfBuckets =>
          (0 to numOfBuckets).map(_.toDouble / numOfBuckets).toArray
        }

        val probabilityArray = probArrayPerCol.flatten.sorted.distinct

        val splitsArrayRaw =
          sparklyr.WeightedQuantileSummaries.approxWeightedQuantile(
            dataset.toDF,
            $(inputCols),
            $(weightCol),
            probabilityArray,
            $(relativeError)
          )

        splitsArrayRaw.zip(probArrayPerCol).map { case (splits, probs) =>
          val probSet = probs.toSet
          val idxSet = probabilityArray.zipWithIndex.collect {
            case (p, idx) if probSet(p) =>
              idx
          }.toSet
          splits.zipWithIndex.collect {
            case (s, idx) if idxSet(idx) =>
              s
          }
        }
      } else {
        sparklyr.WeightedQuantileSummaries.approxWeightedQuantile(
          dataset.toDF,
          $(inputCols),
          $(weightCol),
          (0 to $(numBuckets)).map(_.toDouble / $(numBuckets)).toArray,
          $(relativeError)
        )
      }
      bucketizer.setSplitsArray(splitsArray.map(getDistinctSplits))
    } else {
      val splits =
        sparklyr.WeightedQuantileSummaries.approxWeightedQuantile(
          dataset.toDF,
          Array($(inputCol)),
          $(weightCol),
          (0 to $(numBuckets)).map(_.toDouble / $(numBuckets)).toArray,
          $(relativeError)
        )
      bucketizer.setSplits(getDistinctSplits(splits(0)))
    }
    copyValues(bucketizer.setParent(this))
  }

  private def getDistinctSplits(splits: Array[Double]): Array[Double] = {
    splits(0) = Double.NegativeInfinity
    splits(splits.length - 1) = Double.PositiveInfinity

    // 0.0 and -0.0 are distinct values, array.distinct will preserve both of them.
    // but 0.0 > -0.0 is False which will break the parameter validation checking.
    // and in scala <= 2.12, there's bug which will cause array.distinct generate
    // non-deterministic results when array contains both 0.0 and -0.0
    // So that here we should first normalize all 0.0 and -0.0 to be 0.0
    // See https://github.com/scala/bug/issues/11995
    for (i <- 0 until splits.length) {
      if (splits(i) == -0.0) {
        splits(i) = 0.0
      }
    }
    val distinctSplits = splits.distinct
    if (splits.length != distinctSplits.length) {
      log.warn(s"Some quantiles were identical. Bucketing to ${distinctSplits.length - 1}" +
        s" buckets as a result.")
    }
    distinctSplits.sorted
  }

  override def copy(extra: ParamMap): WeightedQuantileDiscretizer = defaultCopy(extra)
}

object WeightedQuantileDiscretizer extends DefaultParamsReadable[WeightedQuantileDiscretizer] with Logging {
  override def load(path: String): WeightedQuantileDiscretizer = super.load(path)
}
