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
  spark_connection(x$.pipeline)
}

#' @export
ml_save_pipeline <- function(x, path, overwrite = FALSE, ...) {
  UseMethod("ml_save_pipeline")
}

#' @export
ml_save_pipeline.ml_pipeline <- function(x, path, overwrite = FALSE, ...) {
  ensure_scalar_character(path)
  ml_writer <- x$.pipeline %>%
    invoke("write")

  if (overwrite) {
    ml_writer %>%
      invoke("overwrite") %>%
      invoke("save", path)
  } else {
    ml_writer %>%
      invoke("save", path)
  }
}

#' @export
ml_load_pipeline <- function(sc, path) {
  jobj <- invoke_new(sc, "org.apache.spark.ml.Pipeline") %>%
    invoke("load", path)
  ml_pipeline(jobj)
}

#' @export
ml_fit <- function(x, data, ...) {
  jobj <- x$.pipeline %>%
    invoke("fit", spark_dataframe(data))
  stages <- jobj %>%
    invoke("stages") %>%
    sapply(ml_pipeline_stage)

  structure(
    list(stages = stages,
         .pipeline_model = jobj),
    class = "ml_pipeline_model"
  )
}
