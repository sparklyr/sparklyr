package sparklyr

import org.apache.spark.ml._

object MLUtils {
  def wrapInPipeline(pipelineStage: PipelineStage): Pipeline = {
    new Pipeline()
      .setStages(Array(pipelineStage))
  }

  def getParamMap(pipelineStage: PipelineStage): Map[String, Any] = {
    Map(pipelineStage.extractParamMap.toSeq map {
      pair => pair.param.name -> pair.value}: _*)
  }
}
