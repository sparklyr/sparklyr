package sparklyr

import org.apache.spark.mllib.random.RandomDataGenerator
import org.apache.commons.math3.distribution._

trait RealDistributionGenerator extends RandomDataGenerator[Double] {
  protected val dist: AbstractRealDistribution

  override def nextValue(): Double = dist.sample

  override def setSeed(seed: Long): Unit = {
    dist.reseedRandomGenerator(seed)
  }
}

class UniformGenerator(val lower: Double, val upper: Double) extends RealDistributionGenerator {
  override val dist = new UniformRealDistribution(lower, upper)

  override def copy(): UniformGenerator = new UniformGenerator(lower, upper)
}

class NormalGenerator(val mean: Double, val sd: Double) extends RealDistributionGenerator {
  override val dist = new NormalDistribution(mean, sd)

  override def copy(): NormalGenerator = new NormalGenerator(mean, sd)
}
