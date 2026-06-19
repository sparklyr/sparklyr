#' Feature Transformation -- StandardScaler (Estimator)
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
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' features <- c("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width")
#'
#' iris_tbl %>%
#'   ft_vector_assembler(
#'     input_col = features,
#'     output_col = "features_temp"
#'   ) %>%
#'   ft_standard_scaler(
#'     input_col = "features_temp",
#'     output_col = "features",
#'     with_mean = TRUE
#'   )
#' }
#'
#' @export
ft_standard_scaler <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  with_mean = FALSE,
  with_std = TRUE,
  uid = random_string("standard_scaler_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_standard_scaler")
}

ml_standard_scaler <- ft_standard_scaler

ft_standard_scaler_impl <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  with_mean = FALSE,
  with_std = TRUE,
  uid = random_string("standard_scaler_"),
  ...
) {
  ml_process_feature(
    x = x,
    r_class = "ml_standard_scaler",
    uid = uid,
    stage_constructor = new_ml_standard_scaler,
    invoke_steps = list(
      input_col = input_col,
      output_col = output_col,
      with_mean = with_mean,
      with_std = with_std
    )
  )
}

#' @export
ft_standard_scaler.spark_connection <- ft_standard_scaler_impl

#' @export
ft_standard_scaler.ml_pipeline <- ft_standard_scaler_impl

#' @export
ft_standard_scaler.tbl_spark <- ft_standard_scaler_impl

new_ml_standard_scaler <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_standard_scaler")
}

new_ml_standard_scaler_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    mean = possibly_null(read_spark_vector)(jobj, "mean"),
    std = possibly_null(read_spark_vector)(jobj, "std"),
    class = "ml_standard_scaler_model"
  )
}
