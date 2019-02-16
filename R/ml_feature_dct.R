#' Feature Transformation -- Discrete Cosine Transform (DCT) (Transformer)
#'
#' A feature transformer that takes the 1D discrete cosine transform of a real
#'   vector. No zero padding is performed on the input vector. It returns a real
#'   vector of the same length representing the DCT. The return vector is scaled
#'   such that the transform matrix is unitary (aka scaled DCT-II).
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param inverse Indicates whether to perform the inverse DCT (TRUE) or forward DCT (FALSE).
#' @export
ft_dct <- function(x, input_col = NULL, output_col = NULL,
                   inverse = FALSE, uid = random_string("dct_"), ...) {
  check_dots_used()
  UseMethod("ft_dct")
}

ml_dct <- ft_dct

#' @export
ft_dct.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                    inverse = FALSE, uid = random_string("dct_"), ...) {

  .args <- list(
    input_col = input_col,
    output_col = output_col,
    inverse = inverse,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_dct()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.DCT",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]) %>%
    invoke("setInverse", .args[["inverse"]])

  new_ml_dct(jobj)
}

#' @export
ft_dct.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                               inverse = FALSE, uid = random_string("dct_"), ...) {
  transformer <- ft_dct.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    inverse = inverse,
    uid = uid
  )
  ml_add_stage(x, transformer)
}

#' @export
ft_dct.tbl_spark <- function(x, input_col = NULL, output_col = NULL,
                             inverse = FALSE, uid = random_string("dct_"), ...) {
  transformer <- ft_dct.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    inverse = inverse,
    uid = uid
  )
  ml_transform(transformer, x)
}

new_ml_dct <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_dct")
}

#' @rdname ft_dct
#' @details \code{ft_discrete_cosine_transform()} is an alias for \code{ft_dct} for backwards compatibility.
#' @export
ft_discrete_cosine_transform <- function(x, input_col, output_col, inverse = FALSE, uid = random_string("dct_"), ...) {
  UseMethod("ft_dct")
}

validator_ml_dct <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["inverse"]] <- cast_scalar_logical(.args[["inverse"]])
  .args
}
