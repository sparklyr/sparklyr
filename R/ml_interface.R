ml_model <- function(class, model, ...) {
  object <- list(..., .model = model)
  class(object) <- c(
    paste("ml_model", class, sep = "_"),
    "ml_model"
  )
  object
}

#' @export
features <- function(object, ...) {
  UseMethod("features")
}

#' @export
features.default <- function(object, ...) {
  object$features
}

#' @export
response <- function(object, ...) {
  UseMethod("response")
}

#' @export
response.default <- function(object, ...) {
  object$response
}
