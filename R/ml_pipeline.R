#' @export
ml_stages <- function(x, ...) {
  sc <- spark_connection(x$.pipeline)
  dots <- list(...) %>%
    lapply(function(x) x$.pipeline)
  pipeline <- invoke_static(sc,
                            "sparklyr.MLUtils",
                            "composeStages",
                            x$.pipeline, dots)
  stages <- invoke_static(sc,
                          "sparklyr.MLUtils",
                          "explodePipeline",
                          pipeline) %>%
    lapply(ml_pipeline_stage)

  structure(
    list(
      stages = unlist(stages, recursive = FALSE),
      .pipeline = pipeline),
    class = "ml_pipeline"
  )

}
