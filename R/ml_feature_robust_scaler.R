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
ft_robust_scaler <- function(x, input_col = NULL, output_col = NULL,
                             lower = 0.25, upper = 0.75, with_centering = TRUE,
                             with_scaling = TRUE, relative_error = 0.001,
                             uid = random_string("ft_robust_scaler_"), ...) {
  check_dots_used()
  UseMethod("ft_robust_scaler")
}

ml_robust_scaler <- ft_robust_scaler

#' @export
ft_robust_scaler.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                              lower = 0.25, upper = 0.75, with_centering = TRUE,
                                              with_scaling = TRUE, relative_error = 0.001,
                                              uid = random_string("ft_robust_scaler_"), ...) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    lower = lower,
    upper = upper,
    with_centering = with_centering,
    with_scaling = with_scaling,
    relative_error = relative_error,
    uid = uid
  )%>%
    c(rlang::dots_list(...)) %>%
    validator_ml_robust_scaler()

  estimator <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.RobustScaler",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    invoke("setLower", .args[["lower"]]) %>%
    invoke("setUpper", .args[["upper"]]) %>%
    invoke("setWithCentering", .args[["with_centering"]]) %>%
    invoke("setWithScaling", .args[["with_scaling"]]) %>%
    invoke("setRelativeError", .args[["relative_error"]]) %>%
    new_ml_robust_scaler()

  estimator
}

#' @export
ft_robust_scaler.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                                         lower = 0.25, upper = 0.75, with_centering = TRUE,
                                         with_scaling = TRUE, relative_error = 0.001,
                                         uid = random_string("ft_robust_scaler_"), ...) {

  stage <- ft_robust_scaler.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    lower = lower,
    upper = upper,
    with_centering = with_centering,
    with_scaling = with_scaling,
    relative_error = relative_error,
    uid = uid,
    ...
  )

  ml_add_stage(x, stage)
}

#' @export
ft_robust_scaler.tbl_spark <- function(x, input_col = NULL, output_col = NULL,
                                        lower = 0.25, upper = 0.75, with_centering = TRUE,
                                        with_scaling = TRUE, relative_error = 0.001,
                                        uid = random_string("ft_robust_scaler_"), ...) {
  stage <- ft_robust_scaler.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    lower = lower,
    upper = upper,
    with_centering = with_centering,
    with_scaling = with_scaling,
    relative_error = relative_error,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

new_ml_robust_scaler <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_robust_scaler")
}

new_ml_robust_scaler_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_robust_scaler")
}

validator_ml_robust_scaler <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["lower"]] <- cast_scalar_double(.args[["lower"]])
  .args[["upper"]] <- cast_scalar_double(.args[["upper"]])
  .args[["with_centering"]] <- cast_scalar_logical(.args[["with_centering"]])
  .args[["with_scaling"]] <- cast_scalar_logical(.args[["with_scaling"]])
  .args[["relative_error"]] <- cast_scalar_double(.args[["relative_error"]])
  .args
}
