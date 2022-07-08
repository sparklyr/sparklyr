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
                       missing_value = NULL, strategy = "mean",
                       uid = random_string("imputer_"), ...) {
  check_dots_used()
  UseMethod("ft_imputer")
}

ft_imputer_impl <- function(x, input_cols = NULL, output_cols = NULL,
                            missing_value = NULL, strategy = "mean",
                            uid = random_string("imputer_"), ...) {

  spark_require_version(spark_connection(x), "2.2.0", "Imputer")

  estimator_process(
    x = x,
    uid = uid,
    spark_class = "org.apache.spark.ml.feature.Imputer",
    r_class = "ml_imputer",
    invoke_steps = list(
      setInputCols = cast_nullable_string_list(input_cols),
      setOutputCols = cast_nullable_string_list(output_cols),
      setMissingValue = cast_nullable_scalar_double(missing_value),
      setStrategy = cast_choice(strategy, c("mean", "median"))
    )
  )
}

ml_imputer <- ft_imputer

#' @export
ft_imputer.spark_connection <- ft_imputer_impl

#' @export
ft_imputer.ml_pipeline <- ft_imputer_impl

#' @export
ft_imputer.tbl_spark <- ft_imputer_impl

new_ml_imputer_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_imputer_model")
}
