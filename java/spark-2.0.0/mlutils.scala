package sparklyr


import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.ml.linalg._
object MLUtils2 {
  def sparkVector(v: Array[Double]): Vector = {
    Vectors.dense(v)
  }

  def setScalingVec(elementwiseProduct: ElementwiseProduct,
    v: Array[Double]): ElementwiseProduct = {
    elementwiseProduct.setScalingVec(sparkVector(v))
  }
}
