package sparklyr

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object RandomRDDs {
  def betaRDD(
      sc: SparkContext,
      alpha: Double,
      beta: Double,
      n: Long,
      numPartitions: Int,
      seed: Long): RDD[Double] = {
    val generator = new BetaGenerator(alpha, beta)
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, n, numPartitions, seed
    )
  }

  def binomialRDD(
      sc: SparkContext,
      trials: Int,
      p: Double,
      n: Long,
      numPartitions: Int,
      seed: Long): RDD[Int] = {
    val generator = new BinomialGenerator(trials, p)
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, n, numPartitions, seed
    )
  }

  def cauchyRDD(
      sc: SparkContext,
      median: Double,
      scale: Double,
      n: Long,
      numPartitions: Int,
      seed: Long): RDD[Double] = {
    val generator = new CauchyGenerator(median, scale)
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, n, numPartitions, seed
    )
  }

  def chiSquaredRDD(
      sc: SparkContext,
      degreesOfFreedom: Double,
      n: Long,
      numPartitions: Int,
      seed: Long): RDD[Double] = {
    val generator = new ChiSquaredGenerator(degreesOfFreedom)
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, n, numPartitions, seed
    )
  }

  def geometricRDD(
      sc: SparkContext,
      p: Double,
      n: Long,
      numPartitions: Int,
      seed: Long): RDD[Int] = {
    val generator = new GeometricGenerator(p)
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, n, numPartitions, seed
    )
  }

  def hypergeometricRDD(
      sc: SparkContext,
      populationSize: Int,
      numSuccesses: Int,
      numDraws: Int,
      n: Long,
      numPartitions: Int,
      seed: Long): RDD[Int] = {
    val generator = new HypergeometricGenerator(
      populationSize, numSuccesses, numDraws
    )
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, n, numPartitions, seed
    )
  }

  def normalRDD(
      sc: SparkContext,
      mean: Double,
      sd: Double,
      n: Long,
      numPartitions: Int,
      seed: Long): RDD[Double] = {
    val generator = new NormalGenerator(mean, sd)
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, n, numPartitions, seed
    )
  }

  def tRDD(
      sc: SparkContext,
      degreesOfFreedom: Double,
      n: Long,
      numPartitions: Int,
      seed: Long): RDD[Double] = {
    val generator = new TGenerator(degreesOfFreedom)
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, n, numPartitions, seed
    )
  }

  def weibullRDD(
      sc: SparkContext,
      alpha: Double,
      beta: Double,
      n: Long,
      numPartitions: Int,
      seed: Long): RDD[Double] = {
    val generator = new WeibullGenerator(alpha, beta)
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, n, numPartitions, seed
    )
  }

  def uniformRDD(
      sc: SparkContext,
      min: Double,
      max: Double,
      n: Long,
      numPartitions: Int,
      seed: Long): RDD[Double] = {
    val generator = new UniformGenerator(min, max)
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, n, numPartitions, seed
    )
  }
}
