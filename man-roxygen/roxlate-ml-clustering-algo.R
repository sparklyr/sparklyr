#' @param x A \code{spark_connection}, \code{ml_pipeline}, or a \code{tbl_spark}.
#' @param uid A character string used to uniquely identify the ML estimator.
#' @param ... Optional arguments, see Details.
#'
#' @seealso See \url{https://spark.apache.org/docs/latest/ml-clustering.html} for
#'   more information on the set of clustering algorithms.
#'
#' @family ml clustering algorithms
#'
#' @return The object returned depends on the class of \code{x}.
#'
#' \itemize{
#'   \item \code{spark_connection}: When \code{x} is a \code{spark_connection}, the function returns an instance of a \code{ml_estimator} object. The object contains a pointer to
#'   a Spark \code{Estimator} object and can be used to compose
#'   \code{Pipeline} objects.
#'
#'   \item \code{ml_pipeline}: When \code{x} is a \code{ml_pipeline}, the function returns a \code{ml_pipeline} with
#'   the clustering estimator appended to the pipeline.
#'
#'   \item \code{tbl_spark}: When \code{x} is a \code{tbl_spark}, an estimator is constructed then
#'   immediately fit with the input \code{tbl_spark}, returning a clustering model.
#'
#'   \item \code{tbl_spark}, with \code{formula} or \code{features} specified: When \code{formula}
#'     is specified, the input \code{tbl_spark} is first transformed using a
#'     \code{RFormula} transformer before being fit by
#'     the estimator. The object returned in this case is a \code{ml_model} which is a
#'     wrapper of a \code{ml_pipeline_model}. This signature does not apply to \code{ml_lda()}.
#' }
#'

