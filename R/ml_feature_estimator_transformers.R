#' Feature Tranformation -- StringIndexer (Estimator)
#'
#' A label indexer that maps a string column of labels to an ML column of
#'   label indices. If the input column is numeric, we cast it to string and
#'   index the string values. The indices are in \code{[0, numLabels)}, ordered by
#'   label frequencies. So the most frequent label gets index 0. This function
#'   is the inverse of \code{\link{ft_index_to_string}}.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @template roxlate-ml-feature-handle-invalid
#' @seealso \code{\link{ft_index_to_string}}
#' @export
ft_string_indexer <- function(
  x, input_col, output_col,
  handle_invalid = "error", dataset = NULL,
  uid = random_string("string_indexer_"), ...) {
  UseMethod("ft_string_indexer")
}

#' @export
ft_string_indexer.spark_connection <- function(
  x, input_col, output_col,
  handle_invalid = "error", dataset = NULL,
  uid = random_string("string_indexer_"), ...) {

  ml_ratify_args()

  estimator <- ml_new_transformer(x, "org.apache.spark.ml.feature.StringIndexer",
                                  input_col, output_col, uid) %>%
    invoke("setHandleInvalid", handle_invalid) %>%
    new_ml_estimator()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_string_indexer.ml_pipeline <- function(
  x, input_col, output_col,
  handle_invalid = "error", dataset = NULL,
  uid = random_string("string_indexer_"), ...
) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)

}

#' @export
ft_string_indexer.tbl_spark <- function(
  x, input_col, output_col,
  handle_invalid = "error", dataset = NULL,
  uid = random_string("string_indexer_"), ...
) {
  dots <- rlang::dots_list(...)

  stage <- ml_new_stage_modified_args()

  # backwards compatibility for params argument
  if (rlang::has_name(dots, "params") && rlang::is_env(dots$params)) {
    transformer <- if (is_ml_transformer(stage))
      stage
    else
      ml_fit(stage, x)
    dots$params$labels <- spark_jobj(transformer) %>%
      invoke("labels") %>%
      as.character()
    transformer %>%
      ml_transform(x)
  } else {
    if (is_ml_transformer(stage))
      ml_transform(stage, x)
    else
      ml_fit_and_transform(stage, x)
  }
}

#' Feature Transformation -- QuantileDiscretizer (Estimator)
#'
#' \code{ft_quantile_discretizer} takes a column with continuous features and outputs
#'   a column with binned categorical features. The number of bins can be
#'   set using the \code{num_buckets} parameter. It is possible that the number
#'   of buckets used will be smaller than this value, for example, if there
#'   are too few distinct values of the input to create enough distinct
#'   quantiles.
#'
#'   NaN handling: null and NaN values will be ignored from the column
#'   during \code{QuantileDiscretizer} fitting. This will produce a \code{Bucketizer}
#'   model for making predictions. During the transformation, \code{Bucketizer}
#'   will raise an error when it finds NaN values in the dataset, but the
#'   user can also choose to either keep or remove NaN values within the
#'   dataset by setting \code{handle_invalid} If the user chooses to keep NaN values,
#'   they will be handled specially and placed into their own bucket,
#'   for example, if 4 buckets are used, then non-NaN data will be put
#'   into buckets[0-3], but NaNs will be counted in a special bucket[4].
#'
#'   Algorithm: The bin ranges are chosen using an approximate algorithm (see
#'   the documentation for org.apache.spark.sql.DataFrameStatFunctions.approxQuantile
#'   \link[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameStatFunctions]{here} for a detailed description). The precision of the approximation can be
#'   controlled with the \code{relative_error} parameter. The lower and upper bin
#'   bounds will be -Infinity and +Infinity, covering all real values.
#'
#'   Note that the result may be different every time you run it, since the sample
#'   strategy behind it is non-deterministic.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @template roxlate-ml-feature-handle-invalid
#'
#' @param num_buckets Number of buckets (quantiles, or categories) into which data
#'   points are grouped. Must be greater than or equal to 2.
#' @param relative_error Relative error (see documentation for
#'   org.apache.spark.sql.DataFrameStatFunctions.approxQuantile
#'   \link[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameStatFunctions]{here}
#'   for description). Must be in the range [0, 1]. default: 0.001
#'
#' @seealso \code{\link{ft_bucketizer}}
#' @export
ft_quantile_discretizer <- function(
  x, input_col, output_col, handle_invalid = "error",
  num_buckets = 2L, relative_error = 0.001, dataset = NULL,
  uid = random_string("quantile_discretizer_"), ...) {
  UseMethod("ft_quantile_discretizer")
}

#' @export
ft_quantile_discretizer.spark_connection <- function(
  x, input_col, output_col, handle_invalid = "error",
  num_buckets = 2L, relative_error = 0.001, dataset = NULL,
  uid = random_string("quantile_discretizer_"), ...) {

  ml_ratify_args()
  estimator <- ml_new_transformer(x, "org.apache.spark.ml.feature.QuantileDiscretizer",
                                  input_col, output_col, uid) %>%
    invoke("setHandleInvalid", handle_invalid) %>%
    invoke("setNumBuckets", num_buckets) %>%
    invoke("setRelativeError", relative_error) %>%
    new_ml_estimator()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_quantile_discretizer.ml_pipeline <- function(
  x, input_col, output_col, handle_invalid = "error",
  num_buckets = 2L, relative_error = 0.001, dataset = NULL,
  uid = random_string("quantile_discretizer_"), ...) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)
}

#' @export
ft_quantile_discretizer.tbl_spark <- function(
  x, input_col, output_col, handle_invalid = "error",
  num_buckets = 2L, relative_error = 0.001, dataset = NULL,
  uid = random_string("quantile_discretizer_"), ...) {
  stage <- ml_new_stage_modified_args()

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}
