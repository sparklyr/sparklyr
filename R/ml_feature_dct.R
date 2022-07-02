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

ft_dct_impl <- function(x, input_col = NULL, output_col = NULL,
                        inverse = FALSE, uid = random_string("dct_"), ...) {
  ft_process(
    x = x,
    uid = uid,
    spark_class = "org.apache.spark.ml.feature.DCT",
    r_class = "ml_dct",
    invoke_steps = list(
      setInputCol = cast_nullable_string(input_col),
      setOutputCol = cast_nullable_string(output_col),
      setInverse = cast_scalar_logical(inverse)
    )
  )
}

ml_dct <- ft_dct

#' @export
ft_dct.spark_connection <- ft_dct_impl

#' @export
ft_dct.ml_pipeline <- ft_dct_impl

#' @export
ft_dct.tbl_spark <- ft_dct_impl


#' @rdname ft_dct
#' @details \code{ft_discrete_cosine_transform()} is an alias for \code{ft_dct} for backwards compatibility.
#' @export
ft_discrete_cosine_transform <- function(x, input_col, output_col,
                                         inverse = FALSE,
                                         uid = random_string("dct_"), ...) {
  UseMethod("ft_dct")
}
