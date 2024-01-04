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
    ...) {
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
    ...) {
  if (p < 1) stop("`p` must be at least 1.")
  ft_process_step(
    x = x,
    uid = uid,
    r_class = "ml_normalizer",
    step_class = "ml_normalizer",
    invoke_steps = list(
      p = cast_scalar_double(p),
      input_col = cast_nullable_string(input_col),
      output_col = cast_nullable_string(output_col)
    )
  )
}

#' @export
ft_normalizer.spark_connection <- ft_normalizer_impl

#' @export
ft_normalizer.ml_pipeline <- ft_normalizer_impl

#' @export
ft_normalizer.tbl_spark <- ft_normalizer_impl
