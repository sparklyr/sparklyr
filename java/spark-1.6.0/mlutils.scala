package sparklyr

import org.apache.spark.ml._
import scala.util.{Try, Success, Failure}
import org.apache.spark.ml.param._

object MLUtils {
  def createPipelineFromStages(uid: String, stages: PipelineStage*): Pipeline = {
    new Pipeline(uid)
      .setStages(stages.toArray)
  }

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

  def getStages(stage: PipelineStage): Array[_ <: PipelineStage] = {
    if (stage.isInstanceOf[PipelineModel]) {
      stage.asInstanceOf[PipelineModel].stages
    } else {
      stage.asInstanceOf[Pipeline].getStages
    }
  }

  def explodePipeline(pipeline: PipelineStage): Array[PipelineStage] = {
    def f(stage: PipelineStage): Array[PipelineStage] = {
      val pipeline = Try(getStages(stage))
      pipeline match {
        case Failure(s) => Array(stage)
        case Success(s) => s.flatMap(f)
      }
    }
    f(pipeline) filterNot {_.isInstanceOf[Pipeline]}
  }

  def paramMapsToList(paramMaps: Array[ParamMap]): Seq[Map[String, Any]] = {
    paramMaps.toSeq.map(x => Map(x.toSeq map {
      pair => pair.param.name -> pair.value}: _*))
  }

  def paramMapToList(paramMap: ParamMap): Array[(String, String, Any)] = {
    paramMap.toSeq.map(
      pair => (pair.param.parent, pair.param.name, pair.value)
      ).toArray
  }
}
