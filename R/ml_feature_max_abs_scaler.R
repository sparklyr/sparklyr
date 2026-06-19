#' Feature Transformation -- MaxAbsScaler (Estimator)
#'
#' Rescale each feature individually to range [-1, 1] by dividing through the
#'   largest maximum absolute value in each feature. It does not shift/center the
#'   data, and thus does not destroy any sparsity.
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
#'   ft_max_abs_scaler(
#'     input_col = "features_temp",
#'     output_col = "features"
#'   )
#' }
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @export
ft_max_abs_scaler <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  uid = random_string("max_abs_scaler_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_max_abs_scaler")
}

ml_max_abs_scaler <- ft_max_abs_scaler

ft_max_abs_scaler_impl <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  uid = random_string("max_abs_scaler_"),
  ...
) {
  spark_require_version(spark_connection(x), "2.0.0", "MaxAbsScaler")

  ml_process_feature(
    x = x,
    r_class = "ml_max_abs_scaler",
    uid = uid,
    stage_constructor = new_ml_max_abs_scaler,
    invoke_steps = list(
      input_col = input_col,
      output_col = output_col
    )
  )
}

#' @export
ft_max_abs_scaler.spark_connection <- ft_max_abs_scaler_impl

#' @export
ft_max_abs_scaler.ml_pipeline <- ft_max_abs_scaler_impl

#' @export
ft_max_abs_scaler.tbl_spark <- ft_max_abs_scaler_impl

new_ml_max_abs_scaler <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_max_abs_scaler")
}

new_ml_max_abs_scaler_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_max_abs_scaler_model")
}
