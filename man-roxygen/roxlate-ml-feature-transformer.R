#' @param x A \code{spark_connection}, \code{ml_pipeline}, or a \code{tbl_spark}.
#' @param uid A character string used to uniquely identify the feature transformer.
#' @param ... Optional arguments; currently unused.
#' @family feature transformers
#' @return The object returned depends on the class of \code{x}. If it is a
#' \code{spark_connection}, the function returns a \code{ml_estimator} or a
#' \code{ml_estimator} object. If it is a \code{ml_pipeline}, it will return
#' a pipeline with the transformer or estimator appended to it. If a
#' \code{tbl_spark}, it will return a \code{tbl_spark} with the transformation
#'  applied to it.
