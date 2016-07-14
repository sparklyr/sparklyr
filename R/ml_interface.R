ml_model <- function(class, model, ...) {
  call <- sys.call(sys.parent())
  object <- list(..., .call = call, .model = model)
  class(object) <- c(
    paste("ml_model", class, sep = "_"),
    "ml_model"
  )
  object
}

#' @export
spark_jobj.ml_model <- function(x, ...) {
  x$.model
}

#' @export
spark_connection.ml_model <- function(x, ...) {
  spark_connection(x$.model)
}
