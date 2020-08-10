#' Feature Transformation -- Tokenizer (Transformer)
#'
#' A tokenizer that converts the input string to lowercase and then splits it
#' by white spaces.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @export
ft_tokenizer <- function(x, input_col = NULL, output_col = NULL,
                         uid = random_string("tokenizer_"), ...) {
  check_dots_used()
  UseMethod("ft_tokenizer")
}

ml_tokenizer <- ft_tokenizer

#' @export
ft_tokenizer.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                          uid = random_string("tokenizer_"), ...) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_tokenizer()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.Tokenizer",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  )

  new_ml_tokenizer(jobj)
}

#' @export
ft_tokenizer.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                                     uid = random_string("tokenizer_"), ...) {
  stage <- ft_tokenizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_tokenizer.tbl_spark <- function(x, input_col = NULL, output_col = NULL,
                                   uid = random_string("tokenizer_"), ...) {
  stage <- ft_tokenizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_tokenizer <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_tokenizer")
}

validator_ml_tokenizer <- function(.args) {
  validate_args_transformer(.args)
}
