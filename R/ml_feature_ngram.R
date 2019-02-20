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

ml_ngram <- ft_ngram

#' @export
ft_ngram.spark_connection <- function(x, input_col = NULL, output_col = NULL, n = 2,
                                      uid = random_string("ngram_"), ...) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    n = n,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_ngram()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.NGram",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    invoke("setN", .args[["n"]])

  new_ml_ngram(jobj)
}

#' @export
ft_ngram.ml_pipeline <- function(x, input_col = NULL, output_col = NULL, n = 2,
                                 uid = random_string("ngram_"), ...) {

  stage <- ft_ngram.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    n = n,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_ngram.tbl_spark <- function(x, input_col = NULL, output_col = NULL, n = 2,
                               uid = random_string("ngram_"), ...) {
  stage <- ft_ngram.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    n = n,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_ngram <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_ngram")
}

validator_ml_ngram <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["n"]] <- cast_scalar_integer(.args[["n"]])
  .args
}
