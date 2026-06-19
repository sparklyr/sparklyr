#' Feature Transformation -- Tokenizer (Transformer)
#'
#' A tokenizer that converts the input string to lowercase and then splits it
#' by white spaces.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @export
ft_tokenizer <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  uid = random_string("tokenizer_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_tokenizer")
}

ml_tokenizer <- ft_tokenizer

ft_tokenizer_impl <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  uid = random_string("tokenizer_"),
  ...
) {
  ml_process_feature(
    x = x,
    r_class = "ml_tokenizer",
    uid = uid,
    stage_constructor = new_ml_tokenizer,
    invoke_steps = list(
      input_col = input_col,
      output_col = output_col
    )
  )
}

#' @export
ft_tokenizer.spark_connection <- ft_tokenizer_impl

#' @export
ft_tokenizer.ml_pipeline <- ft_tokenizer_impl

#' @export
ft_tokenizer.tbl_spark <- ft_tokenizer_impl

new_ml_tokenizer <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_tokenizer")
}
