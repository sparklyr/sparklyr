#' @param x A \code{spark_connection}, \code{ml_pipeline}, or a \code{tbl_spark}.
#' @param uid A character string used to uniquely identify the feature transformer.
#' @param ... Optional arguments; currently unused.
#'
#' @seealso See \url{https://spark.apache.org/docs/latest/ml-features.html} for
#'   more information on the set of transformations available for DataFrame
#'   columns in Spark.
#'
#' @family feature transformers
#'
#' @return The object returned depends on the class of \code{x}.
#'
#' \itemize{
#'   \item \code{spark_connection}: When \code{x} is a \code{spark_connection}, the function returns a \code{ml_transformer},
#'   a \code{ml_estimator}, or one of their subclasses. The object contains a pointer to
#'   a Spark \code{Transformer} or \code{Estimator} object and can be used to compose
#'   \code{Pipeline} objects.
#'
#'   \item \code{ml_pipeline}: When \code{x} is a \code{ml_pipeline}, the function returns a \code{ml_pipeline} with
#'   the transformer or estimator appended to the pipeline.
#'
#'   \item \code{tbl_spark}: When \code{x} is a \code{tbl_spark}, a transformer is constructed then
#'   immediately applied to the input \code{tbl_spark}, returning a \code{tbl_spark}
#' }
#'

