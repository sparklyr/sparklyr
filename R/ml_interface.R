#' @export
coef.spark_ml <- function(object, ...) {
  object$coefficients
}

#' @export
coefficients.spark_ml <- function(object, ...) {
  object$coefficients
}

#' @export
residuals.spark_ml <- function(object, ...) {
  object$residuals
}

ml_model <- function(class, model, ...) {
  object <- list(..., .model = model)
  class(object) <- c(
    paste("ml_model", class, sep = "_"),
    "ml_model"
  )
  object
}
