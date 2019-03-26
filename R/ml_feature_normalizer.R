#' Feature Transformation -- Normalizer (Transformer)
#'
#' Normalize a vector to have unit norm using the given p-norm.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param p Normalization in L^p space. Must be >= 1. Defaults to 2.
#'
#' @export
ft_normalizer <- function(x, input_col = NULL, output_col = NULL,
                          p = 2, uid = random_string("normalizer_"), ...) {
  check_dots_used()
  UseMethod("ft_normalizer")
}

ml_normalizer <- ft_normalizer

#' @export
ft_normalizer.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                           p = 2, uid = random_string("normalizer_"), ...) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    p = p,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_normalizer()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.Normalizer",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    invoke("setP", .args[["p"]])

  new_ml_normalizer(jobj)
}

#' @export
ft_normalizer.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                                      p = 2, uid = random_string("normalizer_"), ...) {
  stage <- ft_normalizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    p = p,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_normalizer.tbl_spark <- function(x, input_col = NULL, output_col = NULL,
                                    p = 2, uid = random_string("normalizer_"), ...) {
  stage <- ft_normalizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    p = p,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_normalizer <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_normalizer")
}

validator_ml_normalizer <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["p"]] <- cast_scalar_double(.args[["p"]])
  if (.args[["p"]] < 1) stop("`p` must be at least 1.")
  .args
}
