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
ft_imputer <- function(x, input_cols = NULL, output_cols = NULL,
                       missing_value = NULL, strategy = "mean", dataset = NULL,
                       uid = random_string("imputer_"), ...) {
  UseMethod("ft_imputer")
}

#' @export
ft_imputer.spark_connection <- function(x, input_cols = NULL, output_cols = NULL,
                                        missing_value = NULL, strategy = "mean", dataset = NULL,
                                        uid = random_string("imputer_"), ...) {
  spark_require_version(x, "2.2.0", "Imputer")

  .args <- list(
    input_cols = input_cols,
    output_cols = output_cols,
    missing_value = missing_value,
    strategy = strategy,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_imputer()

  jobj <- ml_new_transformer(
    x, "org.apache.spark.ml.feature.Imputer",
    input_cols = .args[["input_cols"]], output_cols = .args[["output_cols"]], uid = .args[["uid"]]) %>%
    invoke("setStrategy", .args[["strategy"]]) %>%
    maybe_set_param("setMissingValue", .args[["missing_value"]])

  estimator <- new_ml_imputer(jobj)

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_imputer.ml_pipeline <- function(x, input_cols = NULL, output_cols = NULL,
                                   missing_value = NULL, strategy = "mean", dataset = NULL,
                                   uid = random_string("imputer_"), ...) {
  stage <- ft_imputer.spark_connection(
    x = spark_connection(x),
    input_cols = input_cols,
    output_cols = output_cols,
    missing_value = missing_value,
    strategy = strategy,
    dataset = dataset,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_imputer.tbl_spark <- function(x, input_cols = NULL, output_cols = NULL,
                                 missing_value = NULL, strategy = "mean", dataset = NULL,
                                 uid = random_string("imputer_"), ...) {
  stage <- ft_imputer.spark_connection(
    x = spark_connection(x),
    input_cols = input_cols,
    output_cols = output_cols,
    missing_value = missing_value,
    strategy = strategy,
    dataset = dataset,
    uid = uid,
    ...
  )
  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

ml_validator_imputer <- function(.args) {
  .args[["input_cols"]] <- cast_nullable_string_list(.args[["input_cols"]])
  .args[["output_cols"]] <- cast_nullable_string_list(.args[["output_cols"]])
  .args[["strategy"]] <- cast_choice(.args[["strategy"]], c("mean", "median"))
  .args[["missing_value"]] <- cast_nullable_scalar_double(.args[["missing_value"]])
  .args
}

new_ml_imputer <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_imputer")
}

new_ml_imputer_model <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_imputer_model")
}
