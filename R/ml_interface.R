#' Create an ML Model Object
#'
#' Create an ML model object, wrapping the result of a Spark ML routine call.
#' The generated object will be an \R list with S3 classes
#' \code{c("ml_model_<class>", "ml_model")}.
#'
#' @param class The name of the machine learning routine used in the
#'   encompassing model. Note that the model name generated will be
#'   generated as \code{ml_model_<class>}; that is, \code{ml_model}
#'   will be prefixed.
#' @param model The underlying Spark model object.
#' @param ... Additional model information; typically supplied as named
#'   values.
#' @param .call The \R call used in generating this model object (ie,
#'   the top-level \R routine that wraps over the associated Spark ML
#'   routine). Typically used for print output in e.g. \code{print}
#'   and \code{summary} methods.
#'
#' @export
ml_model <- function(class, model, ..., .call = sys.call(sys.parent())) {
  object <- list(..., .call = .call, .model = model)
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
