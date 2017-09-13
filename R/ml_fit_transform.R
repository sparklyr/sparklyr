#' @export
ml_fit <- function(x, data, ...) {
  UseMethod("ml_fit")
}

#' @export
ml_fit.ml_pipeline <- function(x, data, ...) {
  jobj <- x$.jobj %>%
    invoke("fit", spark_dataframe(data))

  ml_pipeline_model_info(jobj)
}

#' @export
ml_fit.ml_estimator <- function(x, data, ...) {
  jobj <- x$.jobj %>%
    invoke("fit", spark_dataframe(data))

  ml_transformer_info(jobj)
}

#' @export
ml_transform <- function(x, data, ...) {
  sdf <- spark_dataframe(data)
  x$.jobj %>%
    invoke("transform", sdf) %>%
    sdf_register()
}

#' @export
ml_fit_and_transform <- function(x, data, ...) {
  sdf <- spark_dataframe(data)
  x$.jobj%>%
    invoke("fit", sdf) %>%
    invoke("transform", sdf) %>%
    sdf_register()
}
