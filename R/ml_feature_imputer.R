#' Feature Transformation -- Imputer (Estimator)
#'
#' Imputation estimator for completing missing values, either using the mean or
#'   the median of the columns in which the missing values are located. The input
#'   columns should be of numeric type. This function requires Spark 2.2.0+.
#'
#' @param input_cols The names of the input columns
#' @param output_cols The names of the output columns.
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @param missing_value The placeholder for the missing values. All occurrences of
#'    \code{missing_value} will be imputed. Note that null values are always treated
#'    as missing.
#' @param strategy The imputation strategy. Currently only "mean" and "median" are
#'   supported. If "mean", then replace missing values using the mean value of the
#'   feature. If "median", then replace missing values using the approximate median
#'   value of the feature. Default: mean
#' @export
ft_imputer <- function(
  x, input_cols, output_cols, missing_value = NULL, strategy = "mean",
  dataset = NULL,
  uid = random_string("imputer_"), ...
) {
  UseMethod("ft_imputer")
}

#' @export
ft_imputer.spark_connection <- function(
  x, input_cols, output_cols, missing_value = NULL, strategy = "mean",
  dataset = NULL,
  uid = random_string("imputer_"), ...
) {

  if (spark_version(x) < "2.2.0") stop("ft_imputer() requires Spark 2.2.0+")

  ml_ratify_args()
  jobj <- invoke_new(x, "org.apache.spark.ml.feature.Imputer", uid) %>%
    invoke("setInputCols", input_cols) %>%
    invoke("setOutputCols", output_cols) %>%
    invoke("setStrategy", strategy)

  if (!rlang::is_null(missing_value))
    jobj <- invoke(jobj, "setMissingValue", missing_value)

  estimator <- new_ml_imputer(jobj)

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_imputer.ml_pipeline <- function(
  x, input_cols, output_cols, missing_value = NULL, strategy = "mean",
  dataset = NULL,
  uid = random_string("imputer_"), ...
) {
  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)
}

#' @export
ft_imputer.tbl_spark <- function(
  x, input_cols, output_cols, missing_value = NULL, strategy = "mean",
  dataset = NULL,
  uid = random_string("imputer_"), ...
) {
  stage <- ml_new_stage_modified_args()

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

ml_validator_imputer <- function(args, nms) {
  args %>%
    ml_validate_args({
      strategy <- rlang::arg_match(strategy, c("mean", "median"))
      if (!rlang::is_null(missing_value))
        missing_value <- ensure_scalar_double(missing_value)
      input_cols <- input_cols %>%
        lapply(ensure_scalar_character)
      output_cols <- lapply(output_cols, ensure_scalar_character)
    }) %>%
    ml_extract_args(nms)
}

new_ml_imputer <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_imputer")
}

new_ml_imputer_model <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_imputer_model")
}
