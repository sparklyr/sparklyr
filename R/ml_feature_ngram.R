#' Feature Tranformation -- NGram (Transformer)
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
ft_ngram <- function(x, input_col, output_col, n = 2L,
                     uid = random_string("ngram_"), ...) {
  UseMethod("ft_ngram")
}

#' @export
ft_ngram.spark_connection <- function(x, input_col, output_col, n = 2L,
                                      uid = random_string("ngram_"), ...) {

  ml_ratify_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.NGram",
                             input_col, output_col, uid) %>%
    invoke("setN", n)

  new_ml_ngram(jobj)
}

#' @export
ft_ngram.ml_pipeline <- function(x, input_col, output_col, n = 2L,
                                 uid = random_string("ngram_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_ngram.tbl_spark <- function(x, input_col, output_col, n = 2L,
                               uid = random_string("ngram_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

ml_validator_ngram <- function(args, nms) {
  args %>%
    ml_validate_args({
      n <- ensure_scalar_integer(n)
    }) %>%
    ml_extract_args(nms)
}

new_ml_ngram <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_ngram")
}
