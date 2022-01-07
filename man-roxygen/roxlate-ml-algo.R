#' @param x A \code{spark_connection}, \code{ml_pipeline}, or a \code{tbl_spark}.
#' @param uid A character string used to uniquely identify the ML estimator.
#' @param ... Optional arguments; see Details.
#'
#' @seealso See \url{https://spark.apache.org/docs/latest/ml-classification-regression.html} for
#'   more information on the set of supervised learning algorithms.
#'
#' @family ml algorithms
#'
#' @return The object returned depends on the class of \code{x}.
#'
#' \itemize{
#'   \item \code{spark_connection}: When \code{x} is a \code{spark_connection}, the function returns an instance of a \code{ml_estimator} object. The object contains a pointer to
#'   a Spark \code{Predictor} object and can be used to compose
#'   \code{Pipeline} objects.
#'
#'   \item \code{ml_pipeline}: When \code{x} is a \code{ml_pipeline}, the function returns a \code{ml_pipeline} with
#'   the predictor appended to the pipeline.
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
#' @details When \code{x} is a \code{tbl_spark} and \code{formula} (alternatively, \code{response} and \code{features}) is specified, the function returns a \code{ml_model} object wrapping a \code{ml_pipeline_model} which contains data pre-processing transformers, the ML predictor, and, for classification models, a post-processing transformer that converts predictions into class labels. For classification, an optional argument \code{predicted_label_col} (defaults to \code{"predicted_label"}) can be used to specify the name of the predicted label column. In addition to the fitted \code{ml_pipeline_model}, \code{ml_model} objects also contain a \code{ml_pipeline} object where the ML predictor stage is an estimator ready to be fit against data. This is utilized by \code{\link{ml_save}} with \code{type = "pipeline"} to faciliate model refresh workflows.

