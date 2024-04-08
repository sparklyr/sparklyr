package sparklyr

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

object StratifiedSamplingUtils {
  def sampleWithoutReplacement(
    df: DataFrame,
    groupVars: Seq[String],
    weightColumn: String,
    k: Int,
    seed: Long
  ): RDD[Row] = {
    unsupportedOperationException
  }

  def sampleWithReplacement(
    df: DataFrame,
    groupVars: Seq[String],
    weightColumn: String,
    k: Int,
    seed: Long
  ): RDD[Row] = {
    unsupportedOperationException
  }

  def sampleFracWithoutReplacement(
    df: DataFrame,
    groupVars: Seq[String],
    weightColumn: String,
    frac: Double,
    seed: Long
  ): RDD[Row] = {
    unsupportedOperationException
  }

  def sampleFracWithReplacement(
    df: DataFrame,
    groupVars: Seq[String],
    weightColumn: String,
    frac: Double,
    seed: Long
  ): RDD[Row] = {
    unsupportedOperationException
  }

  private[this] def unsupportedOperationException(): RDD[Row] = {
    throw new UnsupportedOperationException(
      "Stratified sampling requires Spark 3.0 or above."
    )
  }
}
