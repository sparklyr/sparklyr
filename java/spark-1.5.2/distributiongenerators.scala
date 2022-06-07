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

trait IntegerDistributionGenerator extends RandomDataGenerator[Int] {
  protected val dist: AbstractIntegerDistribution

  override def nextValue(): Int = dist.sample

  override def setSeed(seed: Long): Unit = {
    dist.reseedRandomGenerator(seed)
  }
}

class BetaGenerator(val alpha: Double, val beta: Double) extends RealDistributionGenerator {
  override val dist = new BetaDistribution(alpha, beta)

  override def copy(): BetaGenerator = new BetaGenerator(alpha, beta)
}

class BinomialGenerator(val trials: Int, val p: Double) extends IntegerDistributionGenerator {
  override val dist = new BinomialDistribution(trials, p)

  override def copy(): BinomialGenerator = new BinomialGenerator(trials, p)
}

class CauchyGenerator(val median: Double, val scale: Double) extends RealDistributionGenerator {
  override val dist = new CauchyDistribution(median, scale)

  override def copy(): CauchyGenerator = new CauchyGenerator(median, scale)
}

class ChiSquaredGenerator(val degreesOfFreedom: Double) extends RealDistributionGenerator {
  override val dist = new ChiSquaredDistribution(degreesOfFreedom)

  override def copy(): ChiSquaredGenerator = new ChiSquaredGenerator(degreesOfFreedom)
}

class HypergeometricGenerator(val populationSize: Int, val numSuccesses: Int, val numDraws: Int) extends IntegerDistributionGenerator {
  override val dist = new HypergeometricDistribution(populationSize, numSuccesses, numDraws)

  override def copy(): HypergeometricGenerator = new HypergeometricGenerator(populationSize, numSuccesses, numDraws)
}

class GeometricGenerator(val p: Double) extends IntegerDistributionGenerator {
  override val dist = new GeometricDistribution(p)

  override def copy(): GeometricGenerator = new GeometricGenerator(p)
}

class NormalGenerator(val mean: Double, val sd: Double) extends RealDistributionGenerator {
  override val dist = new NormalDistribution(mean, sd)

  override def copy(): NormalGenerator = new NormalGenerator(mean, sd)
}

class TGenerator(val degreesOfFreedom: Double) extends RealDistributionGenerator {
  override val dist = new TDistribution(degreesOfFreedom)

  override def copy(): TGenerator = new TGenerator(degreesOfFreedom)
}

class WeibullGenerator(val alpha: Double, val beta: Double) extends RealDistributionGenerator {
  override val dist = new WeibullDistribution(alpha, beta)

  override def copy(): WeibullGenerator = new WeibullGenerator(alpha, beta)
}

class UniformGenerator(val lower: Double, val upper: Double) extends RealDistributionGenerator {
  override val dist = new UniformRealDistribution(lower, upper)

  override def copy(): UniformGenerator = new UniformGenerator(lower, upper)
}
