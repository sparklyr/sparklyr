#' Feature Tranformation -- MaxAbsScaler (Estimator)
#'
#' Rescale each feature individually to range [-1, 1] by dividing through the
#'   largest maximum absolute value in each feature. It does not shift/center the
#'   data, and thus does not destroy any sparsity.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @export
ft_max_abs_scaler <- function(
  x, input_col, output_col,
  dataset = NULL,
  uid = random_string("max_abs_scaler_"), ...) {
  UseMethod("ft_max_abs_scaler")
}

#' @export
ft_max_abs_scaler.spark_connection <- function(
  x, input_col, output_col,
  dataset = NULL,
  uid = random_string("max_abs_scaler_"), ...) {

  if (spark_version(x) < "2.0.0")
    stop("ft_max_abs_scaler() requires Spark 2.0.0+")

  ml_ratify_args()

  estimator <- ml_new_transformer(x, "org.apache.spark.ml.feature.MaxAbsScaler",
                                  input_col, output_col, uid) %>%
    new_ml_max_abs_scaler()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_max_abs_scaler.ml_pipeline <- function(
  x, input_col, output_col,
  dataset = NULL,
  uid = random_string("max_abs_scaler_"), ...
) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)

}

#' @export
ft_max_abs_scaler.tbl_spark <- function(
  x, input_col, output_col,
  dataset = NULL,
  uid = random_string("max_abs_scaler_"), ...
) {
  dots <- rlang::dots_list(...)

  stage <- ml_new_stage_modified_args()

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

new_ml_max_abs_scaler <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_max_abs_scaler")
}

new_ml_max_abs_scaler_model <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_max_abs_scaler_model")
}

ml_validator_max_abs_scaler <- function(args, nms) {
  args %>%
    ml_extract_args(nms)
}
