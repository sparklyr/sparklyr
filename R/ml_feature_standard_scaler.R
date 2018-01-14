#' Feature Tranformation -- StandardScaler (Estimator)
#'
#' Standardizes features by removing the mean and scaling to unit variance using
#'   column summary statistics on the samples in the training set. The "unit std"
#'    is computed using the corrected sample standard deviation, which is computed
#'    as the square root of the unbiased sample variance.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @param with_mean Whether to center the data with mean before scaling. It will
#'   build a dense output, so take care when applying to sparse input. Default: FALSE
#' @param with_std Whether to scale the data to unit standard deviation. Default: TRUE
#'
#' @export
ft_standard_scaler <- function(
  x, input_col, output_col,
  with_mean = FALSE, with_std = TRUE, dataset = NULL,
  uid = random_string("standard_scaler_"), ...) {
  UseMethod("ft_standard_scaler")
}

#' @export
ft_standard_scaler.spark_connection <- function(
  x, input_col, output_col,
  with_mean = FALSE, with_std = TRUE, dataset = NULL,
  uid = random_string("standard_scaler_"), ...) {

  ml_ratify_args()

  estimator <- ml_new_transformer(x, "org.apache.spark.ml.feature.StandardScaler",
                                  input_col, output_col, uid) %>%
    invoke("setWithMean", with_mean) %>%
    invoke("setWithStd", with_std) %>%
    new_ml_standard_scaler()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_standard_scaler.ml_pipeline <- function(
  x, input_col, output_col,
  with_mean = FALSE, with_std = TRUE, dataset = NULL,
  uid = random_string("standard_scaler_"), ...
) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)

}

#' @export
ft_standard_scaler.tbl_spark <- function(
  x, input_col, output_col,
  with_mean = FALSE, with_std = TRUE, dataset = NULL,
  uid = random_string("standard_scaler_"), ...
) {
  dots <- rlang::dots_list(...)

  stage <- ml_new_stage_modified_args()

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

new_ml_standard_scaler <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_standard_scaler")
}

new_ml_standard_scaler_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    mean = try_null(read_spark_vector(jobj, "mean")),
    std = try_null(read_spark_vector(jobj, "std")),
    subclass = "ml_standard_scaler_model")
}

ml_validator_standard_scaler <- function(args, nms) {
  args %>%
    ml_validate_args({
      with_mean <- ensure_scalar_boolean(with_mean)
      with_std <- ensure_scalar_boolean(with_std)
    }) %>%
    ml_extract_args(nms)
}
