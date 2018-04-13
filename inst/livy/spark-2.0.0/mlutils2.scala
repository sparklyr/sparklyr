//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//


import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.Params

object MLUtils2 {
  def sparkVector(v: Array[Double]): Vector = {
    Vectors.dense(v)
  }

  def setScalingVec(elementwiseProduct: ElementwiseProduct,
    v: Array[Double]): ElementwiseProduct = {
    elementwiseProduct.setScalingVec(sparkVector(v))
  }

  def setInitialWeights(mlp: MultilayerPerceptronClassifier,
    v: Array[Double]): MultilayerPerceptronClassifier = {
      mlp.setInitialWeights(sparkVector(v))
    }

  def getParamMap[T <: Params](obj: T): Map[String, Any] = {
    Map(obj.extractParamMap.toSeq map {
      pair => pair.param.name -> (pair.value match {
        case v: DenseVector => v.toArray
        case _ => pair.value
      })
    }: _*)
  }
}
