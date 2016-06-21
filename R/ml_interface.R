ml_model <- function(class, model, ...) {
  object <- list(..., .model = model)
  class(object) <- c(
    paste("ml_model", class, sep = "_"),
    "ml_model"
  )
  object
}

#' @export
sparkapi_connection.ml_model <- function(x, ...) {
  sparkapi_connection(x$.model)
}
