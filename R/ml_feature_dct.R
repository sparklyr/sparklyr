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
ft_dct <- function(x, input_col, output_col, inverse = FALSE, uid = random_string("dct_"), ...) {
  UseMethod("ft_dct")
}

#' @export
ft_dct.spark_connection <- function(x, input_col, output_col, inverse = FALSE, uid = random_string("dct_"), ...) {

  ml_ratify_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.DCT",
                             input_col, output_col, uid) %>%
    invoke("setInverse", inverse)

  new_ml_dct(jobj)
}

#' @export
ft_dct.ml_pipeline <- function(x, input_col, output_col, inverse = FALSE, uid = random_string("dct_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_dct.tbl_spark <- function(x, input_col, output_col, inverse = FALSE, uid = random_string("dct_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_dct <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_dct")
}

#' @rdname ft_dct
#' @details \code{ft_discrete_cosine_transform()} is an alias for \code{ft_dct} for backwards compatibility.
#' @export
ft_discrete_cosine_transform <- function(x, input_col, output_col, inverse = FALSE, uid = random_string("dct_"), ...) {
  UseMethod("ft_dct")
}

