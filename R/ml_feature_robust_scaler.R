#' Feature Transformation -- RobustScaler (Estimator)
#'
#' RobustScaler removes the median and scales the data according to the quantile range.
#' The quantile range is by default IQR (Interquartile Range, quantile range between the
#' 1st quartile = 25th quantile and the 3rd quartile = 75th quantile) but can be configured.
#' Centering and scaling happen independently on each feature by computing the relevant
#' statistics on the samples in the training set. Median and quantile range are then
#' stored to be used on later data using the transform method.
#' Note that missing values are ignored in the computation of medians and ranges.
#'
#' @param output_col The name of the output column.
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @param lower Lower quantile to calculate quantile range.
#' @param upper Upper quantile to calculate quantile range.
#' @param with_centering Whether to center data with median.
#' @param with_scaling Whether to scale the data to quantile range.
#' @param relative_error The target relative error for quantile computation.
#'
#' @export
ft_robust_scaler <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  lower = 0.25,
  upper = 0.75,
  with_centering = TRUE,
  with_scaling = TRUE,
  relative_error = 0.001,
  uid = random_string("ft_robust_scaler_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_robust_scaler")
}

ml_robust_scaler <- ft_robust_scaler

ft_robust_scaler_impl <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  lower = 0.25,
  upper = 0.75,
  with_centering = TRUE,
  with_scaling = TRUE,
  relative_error = 0.001,
  uid = random_string("ft_robust_scaler_"),
  ...
) {
  ml_process_feature(
    x = x,
    r_class = "ml_robust_scaler",
    uid = uid,
    stage_constructor = new_ml_robust_scaler,
    invoke_steps = list(
      input_col = input_col,
      output_col = output_col,
      lower = lower,
      upper = upper,
      with_centering = with_centering,
      with_scaling = with_scaling,
      relative_error = relative_error
    )
  )
}

#' @export
ft_robust_scaler.spark_connection <- ft_robust_scaler_impl

#' @export
ft_robust_scaler.ml_pipeline <- ft_robust_scaler_impl

#' @export
ft_robust_scaler.tbl_spark <- ft_robust_scaler_impl

new_ml_robust_scaler <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_robust_scaler")
}

new_ml_robust_scaler_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_robust_scaler_model")
}
