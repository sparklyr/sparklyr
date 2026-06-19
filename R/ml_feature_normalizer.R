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
ft_normalizer <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  p = 2,
  uid = random_string("normalizer_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_normalizer")
}

ml_normalizer <- ft_normalizer

ft_normalizer_impl <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  p = 2,
  uid = random_string("normalizer_"),
  ...
) {
  ml_process_feature(
    x = x,
    r_class = "ml_normalizer",
    uid = uid,
    stage_constructor = new_ml_normalizer,
    invoke_steps = list(
      input_col = input_col,
      output_col = output_col,
      p = p
    )
  )
}

#' @export
ft_normalizer.spark_connection <- ft_normalizer_impl

#' @export
ft_normalizer.ml_pipeline <- ft_normalizer_impl

#' @export
ft_normalizer.tbl_spark <- ft_normalizer_impl

new_ml_normalizer <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_normalizer")
}
