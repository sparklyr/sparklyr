#' @export
ml_save_pipeline <- function(x, path, overwrite = FALSE, ...) {
  UseMethod("ml_save_pipeline")
}

#' @export
ml_save_pipeline.ml_pipeline <- function(x, path, overwrite = FALSE, ...) {
  ensure_scalar_character(path)
  ml_writer <- x$.jobj %>%
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
  ml_info(jobj)
}

#' @export
ml_save_model <- function(x, path, overwrite = FALSE, ...) {
  ensure_scalar_character(path)
  ml_writer <- x$.jobj %>%
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
ml_load_model <- function(sc, path) {
  jobj <- invoke_static(sc, "org.apache.spark.ml.PipelineModel", "load", path)
  ml_info(jobj)
}
