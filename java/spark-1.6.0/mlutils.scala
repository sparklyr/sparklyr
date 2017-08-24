package sparklyr

import org.apache.spark.ml._

object MLUtils {
  def wrapInPipeline(pipelineStage: PipelineStage): Pipeline = {
    new Pipeline()
      .setStages(Array(pipelineStage))
  }
}
