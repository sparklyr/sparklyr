#' Feature Transformation -- NGram (Transformer)
#'
#' A feature transformer that converts the input array of strings into an array
#' of n-grams. Null values in the input array are ignored. It returns an array
#' of n-grams where each n-gram is represented by a space-separated string of words.
#'
#' @details When the input is empty, an empty array is returned. When the input
#' array length is less than n (number of elements per n-gram), no n-grams are
#' returned.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @param n Minimum n-gram length, greater than or equal to 1. Default: 2,
#' bigram features
#'
#' @export
ft_ngram <- function(x, input_col = NULL, output_col = NULL, n = 2,
                     uid = random_string("ngram_"), ...) {
  check_dots_used()
  UseMethod("ft_ngram")
}

ml_ngram <- ft_ngram

ft_ngram_impl <- function(x, input_col = NULL, output_col = NULL, n = 2,
                          uid = random_string("ngram_"), ...) {
  ft_process_step(
    x = x,
    r_class = "ml_ngram",
    step_class = "ml_ngram",
    invoke_steps = list(
      n = cast_scalar_integer(n),
      input_col = cast_nullable_string(input_col),
      output_col = cast_nullable_string(output_col)
    )
  )
}

#' @export
ft_ngram.spark_connection <- ft_ngram_impl

#' @export
ft_ngram.ml_pipeline <- ft_ngram_impl

#' @export
ft_ngram.tbl_spark <- ft_ngram_impl
