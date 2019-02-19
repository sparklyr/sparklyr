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
#'   ft_vector_assembler(input_col = features,
#'                       output_col = "features_temp") %>%
#'   ft_max_abs_scaler(input_col = "features_temp",
#'                      output_col = "features")
#' }
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @export
ft_max_abs_scaler <- function(x, input_col = NULL, output_col = NULL,
                              uid = random_string("max_abs_scaler_"), ...) {
  check_dots_used()
  UseMethod("ft_max_abs_scaler")
}

ml_max_abs_scaler <- ft_max_abs_scaler

#' @export
ft_max_abs_scaler.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                               uid = random_string("max_abs_scaler_"), ...) {
  spark_require_version(x, "2.0.0", "MaxAbsScaler")

  .args <- list(
    input_col = input_col,
    output_col = output_col,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_max_abs_scaler()

  estimator <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.MaxAbsScaler",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    new_ml_max_abs_scaler()


  estimator
}

#' @export
ft_max_abs_scaler.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                                          uid = random_string("max_abs_scaler_"), ...) {

  stage <- ft_max_abs_scaler.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)

}

#' @export
ft_max_abs_scaler.tbl_spark <- function(x, input_col = NULL, output_col = NULL,
                                        uid = random_string("max_abs_scaler_"), ...) {

  stage <- ft_max_abs_scaler.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

new_ml_max_abs_scaler <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_max_abs_scaler")
}

new_ml_max_abs_scaler_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_max_abs_scaler_model")
}

validator_ml_max_abs_scaler <- function(.args) {
  validate_args_transformer(.args)
}
