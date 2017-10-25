#' @param dataset (Optional) A \code{tbl_spark}. If provided, eagerly fit the (estimator)
#'   feature "transformer" against \code{dataset}. See details.
#'
#' @details When \code{dataset} is provided for an estimator transformer, the function
#'   internally calls \code{ml_fit()} against \code{dataset}. Hence, the methods for
#'   \code{spark_connection} and \code{ml_pipeline} will then return a \code{ml_transformer}
#'   and a \code{ml_pipeline} with a \code{ml_transformer} appended, respectively. When
#'   \code{x} is a \code{tbl_spark}, the estimator will be fit against \code{dataset} before
#'   transforming \code{x}.
#'
#'   When \code{dataset} is not specified, the constructor returns a \code{ml_estimator}, and,
#'   in the case where \code{x} is a \code{tbl_spark}, the estimator fits against \code{x} then
#'   to obtain a transformer, which is then immediately used to transform \code{x}.

