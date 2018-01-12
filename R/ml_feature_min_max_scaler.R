#' Feature Tranformation -- MinMaxScaler (Estimator)
#'
#' Rescale each feature individually to a common range [min, max] linearly using
#'   column summary statistics, which is also known as min-max normalization or
#'   Rescaling
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @param max Upper bound after transformation, shared by all features Default: 1.0
#' @param min Lower bound after transformation, shared by all features Default: 0.0
#'
#' @export
ft_min_max_scaler <- function(
  x, input_col, output_col,
  min = 0, max = 1, dataset = NULL,
  uid = random_string("min_max_scaler_"), ...) {
  UseMethod("ft_min_max_scaler")
}

#' @export
ft_min_max_scaler.spark_connection <- function(
  x, input_col, output_col,
  min = 0, max = 1, dataset = NULL,
  uid = random_string("min_max_scaler_"), ...) {

  ml_ratify_args()

  estimator <- ml_new_transformer(x, "org.apache.spark.ml.feature.MinMaxScaler",
                                  input_col, output_col, uid) %>%
    invoke("setMin", min) %>%
    invoke("setMax", max) %>%
    new_ml_min_max_scaler()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_min_max_scaler.ml_pipeline <- function(
  x, input_col, output_col,
  min = 0, max = 1, dataset = NULL,
  uid = random_string("min_max_scaler_"), ...
) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)

}

#' @export
ft_min_max_scaler.tbl_spark <- function(
  x, input_col, output_col,
  min = 0, max = 1, dataset = NULL,
  uid = random_string("min_max_scaler_"), ...
) {
  dots <- rlang::dots_list(...)

  stage <- ml_new_stage_modified_args()

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

new_ml_min_max_scaler <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_min_max_scaler")
}

new_ml_min_max_scaler_model <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_min_max_scaler_model")
}

ml_validator_min_max_scaler <- function(args, nms) {
  args %>%
    ml_validate_args({
      min <- ensure_scalar_double(min)
      max <- ensure_scalar_double(max)
    }) %>%
    ml_extract_args(nms)
}
