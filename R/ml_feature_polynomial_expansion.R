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
ft_polynomial_expansion <- function(x, input_col = NULL, output_col = NULL,
                                    degree = 2, uid = random_string("polynomial_expansion_"), ...) {
  UseMethod("ft_polynomial_expansion")
}

#' @export
ft_polynomial_expansion.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                                     degree = 2, uid = random_string("polynomial_expansion_"), ...) {

  .args <- list(
    input_col = input_col,
    output_col = output_col,
    degree = degree,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_polynomial_expansion()

  jobj <- ml_new_transformer(
    x, "org.apache.spark.ml.feature.PolynomialExpansion",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]) %>%
    invoke("setDegree", .args[["degree"]])

  new_ml_polynomial_expansion(jobj)
}

#' @export
ft_polynomial_expansion.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                                                degree = 2, uid = random_string("polynomial_expansion_"), ...) {
  stage <- ft_polynomial_expansion.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    degree = degree,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_polynomial_expansion.tbl_spark <- function(x, input_col = NULL, output_col = NULL,
                                              degree = 2, uid = random_string("polynomial_expansion_"), ...) {
  stage <- ft_polynomial_expansion.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    degree = degree,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_polynomial_expansion <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_polynomial_expansion")
}

ml_validator_polynomial_expansion <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["degree"]] <- cast_scalar_integer(.args[["degree"]])
  if (.args[["degree"]] < 1) stop("`degree` must be greater than 1.", call. = FALSE)
  .args
}
