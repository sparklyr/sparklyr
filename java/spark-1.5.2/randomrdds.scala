package sparklyr

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RandomRDDs {
  def uniformRDD(
      sc: SparkContext,
      min: Double,
      max: Double,
      size: Long,
      numPartitions: Int,
      seed: Long): RDD[Double] = {
    val generator = new UniformGenerator(min, max)
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, size, numPartitions, seed
    )
  }

  def normalRDD(
      sc: SparkContext,
      mean: Double,
      sd: Double,
      size: Long,
      numPartitions: Int,
      seed: Long): RDD[Double] = {
    val generator = new NormalGenerator(mean, sd)
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, size, numPartitions, seed
    )
  }
}
