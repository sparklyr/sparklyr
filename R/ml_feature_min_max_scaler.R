#' Feature Transformation -- MinMaxScaler (Estimator)
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
#'   ft_min_max_scaler(
#'     input_col = "features_temp",
#'     output_col = "features"
#'   )
#' }
#'
#' @export
ft_min_max_scaler <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  min = 0,
  max = 1,
  uid = random_string("min_max_scaler_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_min_max_scaler")
}

ml_min_max_scaler <- ft_min_max_scaler

ft_min_max_scaler_impl <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  min = 0,
  max = 1,
  uid = random_string("min_max_scaler_"),
  ...
) {
  ml_process_feature(
    x = x,
    r_class = "ml_min_max_scaler",
    uid = uid,
    stage_constructor = new_ml_min_max_scaler,
    invoke_steps = list(
      input_col = input_col,
      output_col = output_col,
      min = min,
      max = max
    )
  )
}

#' @export
ft_min_max_scaler.spark_connection <- ft_min_max_scaler_impl

#' @export
ft_min_max_scaler.ml_pipeline <- ft_min_max_scaler_impl

#' @export
ft_min_max_scaler.tbl_spark <- ft_min_max_scaler_impl

new_ml_min_max_scaler <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_min_max_scaler")
}

new_ml_min_max_scaler_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_min_max_scaler_model")
}
