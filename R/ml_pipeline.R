#' @export
ml_pipeline <- function(x, ..., uid = random_string("pipeline_")) {
  UseMethod("ml_pipeline")
}

#' @export
ml_pipeline.spark_connection <- function(x, uid = random_string("pipeline_")) {
  ensure_scalar_character(uid)
  jobj <- invoke_new(x, "org.apache.spark.ml.Pipeline", uid)
  structure(
    list(uid = invoke(jobj, "uid"),
         stages = NULL,
         type = jobj_info(jobj)$class,
         .jobj = jobj),
    class = c("ml_pipeline", "ml_pipeline_stage")
  )
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
  ml_pipeline_info(jobj)
}

#' @export
ml_stages <- function(x, ...) {
  sc <- spark_connection(x$.pipeline)
  dots <- list(...) %>%
    lapply(function(x) x$.pipeline)
  invoke_static(sc,
                "sparklyr.MLUtils",
                "composeStages",
                x$.pipeline, dots) %>%
    ml_pipeline()
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

#' @export
ml_fit <- function(x, data, ...) {
  jobj <- x$.jobj %>%
    invoke("fit", spark_dataframe(data))

  ml_pipeline_model_info(jobj)
}
