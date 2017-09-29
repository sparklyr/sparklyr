package sparklyr

import org.apache.spark.ml._
import scala.util.{Try, Success, Failure}

object MLUtils {
  def wrapInPipeline(pipelineStage: PipelineStage): Pipeline = {
    if (pipelineStage.isInstanceOf[Pipeline]) {
      pipelineStage.asInstanceOf[Pipeline]
    } else {
      new Pipeline()
      .setStages(Array(pipelineStage))
    }
  }

  def getParamMap(pipelineStage: PipelineStage): Map[String, Any] = {
    Map(pipelineStage.extractParamMap.toSeq map {
      pair => pair.param.name -> pair.value}: _*)
  }

  def composeStages(pipeline: Pipeline, stages: PipelineStage*): Pipeline = {
    new Pipeline()
    .setStages(explodePipeline(pipeline) ++ stages.flatMap(explodePipeline))
  }

  def explodePipeline(pipeline: PipelineStage): Array[PipelineStage] = {
    def f(stage: PipelineStage): Array[PipelineStage] = {
      val pipeline = Try(stage.asInstanceOf[Pipeline].getStages)
      pipeline match {
        case Failure(s) => Array(stage)
        case Success(s) => s.flatMap(f)
      }
    }
    f(pipeline) filterNot {_.isInstanceOf[Pipeline]}
  }
}
