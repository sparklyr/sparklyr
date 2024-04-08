#' @param x A \code{spark_connection}, \code{ml_pipeline}, or a \code{tbl_spark}.
#' @param uid A character string used to uniquely identify the ML estimator.
#' @param ... Optional arguments; see Details.
#' @family ml algorithms
#' @return The object returned depends on the class of \code{x}. If it is a
#' \code{spark_connection}, the function returns a \code{ml_estimator} object. If
#' it is a \code{ml_pipeline}, it will return a pipeline with the predictor
#' appended to it. If a \code{tbl_spark}, it will return a \code{tbl_spark} with
#' the predictions added to it.
