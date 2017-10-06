#' @param x A \code{spark_connection}, \code{ml_pipeline}, or a \code{tbl_spark}.
#' @param uid A character string used to uniquely identify the ML estimator.
#' @param ... Optional arguments; currently unused.
#'
#' @seealso See \url{http://spark.apache.org/docs/latest/ml-classification-regression.html} for
#'   more information on the set of supervised learning algorithms.
#'
#' @family ml algorithms
#'
#' @return The object returned depends on the class of \code{x}.
#'
#' \itemize{
#'   \item \code{spark_connection}: When \code{x} is a \code{spark_connection}, the function returns an instance of a \code{ml_predictor} object. The object contains a pointer to
#'   a Spark \code{Predictor} object and can be used to compose
#'   \code{Pipeline} objects.
#'
#'   \item \code{ml_pipeline}: When \code{x} is a \code{ml_pipeline}, the function returns a \code{ml_pipeline} with
#'   the predictor appended to the pipline.
#'
#'   \item \code{tbl_spark}: When \code{x} is a \code{tbl_spark}, a predictor is constructed then
#'   immediately fit with the input \code{tbl_spark}, returning a prediction model.
#'
#'   \item \code{tbl_spark}, with \code{formula}: specified When \code{formula}
#'     is specified, the input \code{tbl_spark} is first transformed using a
#'     \code{RFormula} transformer before being fit by
#'     the predictor. The object returned in this case is a \code{ml_model} which is a
#'     wrapper of a \code{ml_pipeline_model}.
#' }
#'

