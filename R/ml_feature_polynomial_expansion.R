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
ft_polynomial_expansion <- function(x, input_col, output_col, degree = 2L, uid = random_string("polynomial_expansion_"), ...) {
  UseMethod("ft_polynomial_expansion")
}

#' @export
ft_polynomial_expansion.spark_connection <- function(
  x, input_col, output_col, degree = 2L,
  uid = random_string("polynomial_expansion_"), ...
) {

  ml_ratify_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.PolynomialExpansion",
                             input_col, output_col, uid) %>%
    invoke("setDegree", degree)

  new_ml_polynomial_expansion(jobj)
}

#' @export
ft_polynomial_expansion.ml_pipeline <- function(
  x, input_col, output_col, degree = 2L,
  uid = random_string("polynomial_expansion_"), ...
  ) {
  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_polynomial_expansion.tbl_spark <- function(
  x, input_col, output_col, degree = 2L,
  uid = random_string("polynomial_expansion_"), ...
  ) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_polynomial_expansion <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_polynomial_expansion")
}

ml_validator_polynomial_expansion <- function(args, nms) {
  args %>%
    ml_validate_args({
      degree <- ensure_scalar_integer(degree)
      if (degree < 1) stop("degree should be greater than 1")
    }) %>%
    ml_extract_args(nms)
}
