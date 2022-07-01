#' Feature Transformation -- NGram (Transformer)
#'
#' A feature transformer that converts the input array of strings into an array of n-grams. Null values in the input array are ignored. It returns an array of n-grams where each n-gram is represented by a space-separated string of words.
#'
#' @details When the input is empty, an empty array is returned. When the input array length is less than n (number of elements per n-gram), no n-grams are returned.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @param n Minimum n-gram length, greater than or equal to 1. Default: 2, bigram features
#'
#' @export
ft_ngram <- function(x, input_col = NULL, output_col = NULL, n = 2,
                     uid = random_string("ngram_"), ...) {
  check_dots_used()
  UseMethod("ft_ngram")
}

ft_ngram_impl <- function(x, input_col = NULL, output_col = NULL, n = 2,
                          uid = random_string("ngram_"), ...) {
  ft_process(
    x = x,
    uid = uid,
    spark_class = "org.apache.spark.ml.feature.NGram",
    r_class = "ml_ngram",
    invoke_steps = list(
      setInputCol = cast_nullable_string(input_col),
      setOutputCol = cast_nullable_string(output_col),
      setN = cast_scalar_integer(n)
    )
  )
}

ml_ngram <- ft_ngram

#' @export
ft_ngram.spark_connection <- ft_ngram_impl

#' @export
ft_ngram.ml_pipeline <- ft_ngram_impl

#' @export
ft_ngram.tbl_spark <- ft_ngram_impl
