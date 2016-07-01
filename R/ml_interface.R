ml_model <- function(class, model, ...) {
  object <- list(..., .model = model)
  class(object) <- c(
    paste("ml_model", class, sep = "_"),
    "ml_model"
  )
  object
}

#' @export
sparkapi_jobj.ml_model <- function(x, ...) {
  x$.model
}

#' @export
spark_connection.ml_model <- function(x, ...) {
  spark_connection(x$.model)
}
