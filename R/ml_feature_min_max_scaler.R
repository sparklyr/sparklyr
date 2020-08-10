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
ft_min_max_scaler <- function(x, input_col = NULL, output_col = NULL,
                              min = 0, max = 1,
                              uid = random_string("min_max_scaler_"), ...) {
  check_dots_used()
  UseMethod("ft_min_max_scaler")
}

ml_min_max_scaler <- ft_min_max_scaler

#' @export
ft_min_max_scaler.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                               min = 0, max = 1,
                                               uid = random_string("min_max_scaler_"), ...) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    min = min,
    max = max,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_min_max_scaler()

  estimator <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.MinMaxScaler",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    invoke("setMin", .args[["min"]]) %>%
    invoke("setMax", .args[["max"]]) %>%
    new_ml_min_max_scaler()

  estimator
}

#' @export
ft_min_max_scaler.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                                          min = 0, max = 1,
                                          uid = random_string("min_max_scaler_"), ...) {
  stage <- ft_min_max_scaler.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    min = min,
    max = max,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_min_max_scaler.tbl_spark <- function(x, input_col = NULL, output_col = NULL,
                                        min = 0, max = 1,
                                        uid = random_string("min_max_scaler_"), ...) {
  stage <- ft_min_max_scaler.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    min = min,
    max = max,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_min_max_scaler <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_min_max_scaler")
}

new_ml_min_max_scaler_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_min_max_scaler_model")
}

validator_ml_min_max_scaler <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["min"]] <- cast_scalar_double(.args[["min"]])
  .args[["max"]] <- cast_scalar_double(.args[["max"]])
  .args
}
