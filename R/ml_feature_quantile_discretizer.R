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
#' @param input_cols Names of input columns.
#' @param output_cols Names of output columns.
#' @param num_buckets Number of buckets (quantiles, or categories) into which data
#'   points are grouped. Must be greater than or equal to 2.
#' @param num_buckets_array Array of number of buckets (quantiles, or categories)
#'   into which data points are grouped. Each value must be greater than or equal to 2.
#' @param relative_error (Spark 2.0.0+) Relative error (see documentation for
#'   org.apache.spark.sql.DataFrameStatFunctions.approxQuantile
#'   \href{https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameStatFunctions}{here}
#'   for description). Must be in the range [0, 1]. default: 0.001
#'
#' @seealso \code{\link{ft_bucketizer}}
#' @export
ft_quantile_discretizer <- function(
  x, input_col = NULL, output_col = NULL, num_buckets = 2L,
  input_cols = NULL, output_cols = NULL, num_buckets_array = NULL,
  handle_invalid = "error", relative_error = 0.001, dataset = NULL,
  uid = random_string("quantile_discretizer_"), ...) {
  UseMethod("ft_quantile_discretizer")
}

#' @export
ft_quantile_discretizer.spark_connection <- function(
  x, input_col = NULL, output_col = NULL, num_buckets = 2L,
  input_cols = NULL, output_cols = NULL, num_buckets_array = NULL,
  handle_invalid = "error", relative_error = 0.001, dataset = NULL,
  uid = random_string("quantile_discretizer_"), ...) {

  .args <- list(
    input_col = input_col,
    output_col = output_col,
    num_buckets = num_buckets,
    input_cols = input_cols,
    output_cols = output_cols,
    num_buckets_array = num_buckets_array,
    handle_invalid = handle_invalid,
    relative_error = relative_error,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_quantile_discretizer()

  jobj <- invoke_new(x, "org.apache.spark.ml.feature.QuantileDiscretizer", .args[["uid"]]) %>%
    jobj_set_param("setHandleInvalid", .args[["handle_invalid"]], "error", "2.1.0") %>%
    jobj_set_param("setRelativeError", .args[["relative_error"]], 0.001, "2.0.0")

  if (is.null(.args[["num_buckets_array"]])) {
    jobj <- jobj %>%
      invoke("setInputCol", .args[["input_col"]]) %>%
      invoke("setOutputCol", .args[["output_col"]]) %>%
      invoke("setNumBuckets", .args[["num_buckets"]])
  } else {
    jobj <- jobj %>%
      invoke("setInputCols", .args[["input_cols"]]) %>%
      invoke("setOutputCols", .args[["output_cols"]]) %>%
      invoke("setNumBucketsArray", .args[["num_buckets_array"]])
  }

  estimator <- jobj %>%
    new_ml_quantile_discretizer()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_quantile_discretizer.ml_pipeline <- function(
  x, input_col = NULL, output_col = NULL, num_buckets = 2L,
  input_cols = NULL, output_cols = NULL, num_buckets_array = NULL,
  handle_invalid = "error", relative_error = 0.001, dataset = NULL,
  uid = random_string("quantile_discretizer_"), ...) {

  stage <- ft_quantile_discretizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    num_buckets = num_buckets,
    input_cols = input_cols,
    output_cols = output_cols,
    num_buckets_array = num_buckets_array,
    handle_invalid = handle_invalid,
    relative_error = relative_error,
    dataset = dataset,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_quantile_discretizer.tbl_spark <- function(
  x, input_col = NULL, output_col = NULL, num_buckets = 2L,
  input_cols = NULL, output_cols = NULL, num_buckets_array = NULL,
  handle_invalid = "error", relative_error = 0.001, dataset = NULL,
  uid = random_string("quantile_discretizer_"), ...) {

  stage <- ft_quantile_discretizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    num_buckets = num_buckets,
    input_cols = input_cols,
    output_cols = output_cols,
    num_buckets_array = num_buckets_array,
    handle_invalid = handle_invalid,
    relative_error = relative_error,
    dataset = dataset,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

new_ml_quantile_discretizer <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_quantile_discretizer")
}

ml_validator_quantile_discretizer <- function(.args) {
  .args <- ml_backwards_compatibility(.args, list(
    n.buckets = "num_buckets"
  )) %>%
    ml_backwards_compatibility()

  .args[["uid"]] <- forge::cast_scalar_character(.args[["uid"]])

  if (!is.null(.args[["input_col"]])) {
    if (!is.null(.args[["input_cols"]]))
      stop("Only one of `input_col` or `input_cols` may be specified.", call. = FALSE)
    .args[["input_col"]] <- forge::cast_scalar_character(.args[["input_col"]])
    .args[["output_col"]] <- forge::cast_scalar_character(.args[["output_col"]])
    .args[["num_buckets"]] <- forge::cast_scalar_integer(.args[["num_buckets"]])
  } else if (!is.null(.args[["input_cols"]])) {
    .args[["input_cols"]] <- forge::cast_character(.args[["input_cols"]]) %>% as.list()
    .args[["output_cols"]] <- forge::cast_character(.args[["output_cols"]]) %>% as.list()
    .args[["num_buckets_array"]] <- forge::cast_integer(.args[["num_buckets_array"]]) %>% as.list()
  } else {
    stop("One of `input_col` or `input_cols` must be specified.", call. = FALSE)
  }
  .args[["handle_invalid"]] <- forge::cast_choice(
    .args[["handle_invalid"]],
    c("error", "skip", "keep")
  )
  .args[["relative_error"]] <- forge::cast_scalar_double(.args[["relative_error"]])
  .args
}
