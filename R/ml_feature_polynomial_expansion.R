#' Feature Transformation -- PolynomialExpansion (Transformer)
#'
#' Perform feature expansion in a polynomial space. E.g. take a 2-variable feature
#'   vector as an example: (x, y), if we want to expand it with degree 2, then
#'   we get (x, x * x, y, x * y, y * y).
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param degree The polynomial degree to expand, which should be greater
#'   than equal to 1. A value of 1 means no expansion. Default: 2
#' @export
ft_polynomial_expansion <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  degree = 2,
  uid = random_string("polynomial_expansion_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_polynomial_expansion")
}

ml_polynomial_expansion <- ft_polynomial_expansion

ft_polynomial_expansion_impl <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  degree = 2,
  uid = random_string("polynomial_expansion_"),
  ...
) {
  ml_process_feature(
    x = x,
    r_class = "ml_polynomial_expansion",
    uid = uid,
    stage_constructor = new_ml_polynomial_expansion,
    invoke_steps = list(
      input_col = input_col,
      output_col = output_col,
      degree = degree
    )
  )
}

#' @export
ft_polynomial_expansion.spark_connection <- ft_polynomial_expansion_impl

#' @export
ft_polynomial_expansion.ml_pipeline <- ft_polynomial_expansion_impl

#' @export
ft_polynomial_expansion.tbl_spark <- ft_polynomial_expansion_impl

new_ml_polynomial_expansion <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_polynomial_expansion")
}
