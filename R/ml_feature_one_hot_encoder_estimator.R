#' Feature Transformation -- OneHotEncoderEstimator (Estimator)
#'
#' A one-hot encoder that maps a column of category indices
#'   to a column of binary vectors, with at most a single one-value
#'   per row that indicates the input category index. For example
#'   with 5 categories, an input value of 2.0 would map to an output
#'   vector of [0.0, 0.0, 1.0, 0.0]. The last category is not included
#'   by default (configurable via dropLast), because it makes the
#'   vector entries sum up to one, and hence linearly dependent. So
#'   an input value of 4.0 maps to [0.0, 0.0, 0.0, 0.0].
#'
#' @param input_cols Names of input columns.
#' @param output_cols Names of output columns.
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @template roxlate-ml-feature-handle-invalid
#' @param drop_last Whether to drop the last category. Defaults to \code{TRUE}.
#'
#' @export
ft_one_hot_encoder_estimator <- function(x, input_cols = NULL, output_cols = NULL,
                                          handle_invalid = "error", drop_last = TRUE,
                                          uid = random_string("one_hot_encoder_estimator_"), ...) {
  check_dots_used()
  UseMethod("ft_one_hot_encoder_estimator")
}

ml_one_hot_encoder_estimator <- ft_one_hot_encoder_estimator

#' @export
ft_one_hot_encoder_estimator.spark_connection <- function(x, input_cols = NULL, output_cols = NULL,
                                                           handle_invalid = "error", drop_last = TRUE,
                                                           uid = random_string("one_hot_encoder_estimator_"), ...) {
  spark_require_version(x, "2.3.0")

  .args <- list(
    input_cols = input_cols,
    output_cols = output_cols,
    handle_invalid = handle_invalid,
    drop_last = drop_last,
    uid = uid
  ) %>%
    validator_ml_one_hot_encoder_estimator()

  estimator <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.OneHotEncoderEstimator",
    input_cols = .args[["input_cols"]], output_cols = .args[["output_cols"]], uid = .args[["uid"]]
  ) %>%
    invoke("setHandleInvalid", .args[["handle_invalid"]]) %>%
    invoke("setDropLast", .args[["drop_last"]]) %>%
    new_ml_one_hot_encoder_estimator()

  estimator
}

#' @export
ft_one_hot_encoder_estimator.ml_pipeline <- function(x, input_cols = NULL, output_cols = NULL,
                                                      handle_invalid = "error", drop_last = TRUE,
                                                      uid = random_string("one_hot_encoder_estimator_"), ...) {
  stage <- ft_one_hot_encoder_estimator.spark_connection(
    x = spark_connection(x),
    input_cols = input_cols,
    output_cols = output_cols,
    handle_invalid = handle_invalid,
    drop_last = drop_last,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_one_hot_encoder_estimator.tbl_spark <- function(x, input_cols = NULL, output_cols = NULL,
                                                    handle_invalid = "error", drop_last = TRUE,
                                                    uid = random_string("one_hot_encoder_estimator_"), ...) {
  stage <- ft_one_hot_encoder_estimator.spark_connection(
    x = spark_connection(x),
    input_cols = input_cols,
    output_cols = output_cols,
    handle_invalid = handle_invalid,
    drop_last = drop_last,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_one_hot_encoder_estimator <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_one_hot_encoder_estimator")
}

new_ml_one_hot_encoder_estimator_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    category_size = invoke(jobj, "categorySize"),
    class = "ml_one_hot_encoder_model"
  )
}

validator_ml_one_hot_encoder_estimator <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["handle_invalid"]] <- cast_choice(
    .args[["handle_invalid"]], c("error", "skip", "keep")
  )
  .args[["drop_last"]] <- cast_scalar_logical(.args[["drop_last"]])
  .args
}
