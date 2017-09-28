#' @export
ml_pipeline <- function(x, ..., uid = random_string("pipeline_")) {
  UseMethod("ml_pipeline")
}

#' @export
ml_pipeline.spark_connection <- function(x, uid = random_string("pipeline_")) {
  ensure_scalar_character(uid)
  jobj <- invoke_new(x, "org.apache.spark.ml.Pipeline", uid)
  new_ml_pipeline(jobj)
}

#' @export
ml_pipeline.ml_pipeline_stage <- function(x, ..., uid = random_string("pipeline_")) {
  ensure_scalar_character(uid)
  sc <- spark_connection(x)
  dots <- list(...) %>%
    lapply(function(x) x$.jobj)
  stages <- c(x$.jobj, dots)
  jobj <- invoke_static(sc, "sparklyr.MLUtils",
                        "createPipelineFromStages",
                        uid,
                        stages)
  new_ml_pipeline(jobj)
}

#' @export
spark_connection.ml_pipeline <- function(x, ...) {
  spark_connection(x$.jobj)
}

#' @export
spark_connection.ml_pipeline_stage <- function(x, ...) {
  spark_connection(x$.jobj)
}

#' @export
spark_connection.ml_pipeline_model <- function(x, ...) {
  spark_connection(x$.jobj)
}
