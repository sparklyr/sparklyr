#' Feature Transformation -- Bucketizer (Transformer)
#'
#' Similar to \R's \code{\link{cut}} function, this transforms a numeric column
#' into a discretized column, with breaks specified through the \code{splits}
#' parameter.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-handle-invalid
#'
#' @param splits A numeric vector of cutpoints, indicating the bucket boundaries.
#'
#' @export
ft_bucketizer <- function(
  x, input_col, output_col, splits, handle_invalid = "error",
  uid = random_string("bucketizer_"), ...) {
  UseMethod("ft_bucketizer")
}

#' @export
ft_bucketizer.spark_connection <- function(
  x, input_col, output_col, splits, handle_invalid = "error",
  uid = random_string("bucketizer_"), ...) {

  ml_ratify_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.Bucketizer",
                             input_col, output_col, uid) %>%
    invoke("setSplits", splits) %>%
    jobj_set_param("setHandleInvalid", handle_invalid, "error", "2.1.0")

  new_ml_bucketizer(jobj)
}

#' @export
ft_bucketizer.ml_pipeline <- function(
  x, input_col, output_col, splits, handle_invalid = "error",
  uid = random_string("bucketizer_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_bucketizer.tbl_spark <- function(
  x, input_col, output_col, splits, handle_invalid = "error",
  uid = random_string("bucketizer_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_bucketizer <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_bucketizer")
}

# Validator
ml_validator_bucketizer <- function(args, nms) {
  args %>%
    ml_validate_args({
      if (length(splits) < 3) stop("length(splits) must be at least 3")
      splits <- lapply(splits, ensure_scalar_double)
      handle_invalid <- rlang::arg_match(handle_invalid, c("error", "skip", "keep"))
    }) %>%
    ml_extract_args(nms)
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
#'   \href{https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameStatFunctions}{here} for a detailed description). The precision of the approximation can be
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
#' @param relative_error (Spark 2.0.0+) Relative error (see documentation for
#'   org.apache.spark.sql.DataFrameStatFunctions.approxQuantile
#'   \href{https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameStatFunctions}{here}
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
    jobj_set_param("setHandleInvalid", handle_invalid, "error", "2.1.0") %>%
    invoke("setNumBuckets", num_buckets) %>%
    jobj_set_param("setRelativeError", relative_error, 0.001, "2.0.0") %>%
    new_ml_quantile_discretizer()

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

new_ml_quantile_discretizer <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_quantile_discretizer")
}

# Validator
ml_validator_quantile_discretizer <- function(args, nms) {
  old_new_mapping <- c(
    list(
      n.buckets = "num_buckets"
    ), input_output_mapping
  )

  args %>%
    ml_validate_args(
      {
        handle_invalid <- rlang::arg_match(handle_invalid, c("error", "skip", "keep"))
        num_buckets <- ensure_scalar_integer(num_buckets)
        relative_error <- ensure_scalar_double(relative_error)
      }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}
