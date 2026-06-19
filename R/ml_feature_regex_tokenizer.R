#' Feature Transformation -- RegexTokenizer (Transformer)
#'
#' A regex based tokenizer that extracts tokens either by using the provided
#' regex pattern to split the text (default) or repeatedly matching the regex
#' (if \code{gaps} is false). Optional parameters also allow filtering tokens using a
#' minimal length. It returns an array of strings that can be empty.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param gaps Indicates whether regex splits on gaps (TRUE) or matches tokens (FALSE).
#' @param min_token_length Minimum token length, greater than or equal to 0.
#' @param pattern The regular expression pattern to be used.
#' @param to_lower_case Indicates whether to convert all characters to lowercase before tokenizing.
#'
#' @export
ft_regex_tokenizer <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  gaps = TRUE,
  min_token_length = 1,
  pattern = "\\s+",
  to_lower_case = TRUE,
  uid = random_string("regex_tokenizer_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_regex_tokenizer")
}

ml_regex_tokenizer <- ft_regex_tokenizer

ft_regex_tokenizer_impl <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  gaps = TRUE,
  min_token_length = 1,
  pattern = "\\s+",
  to_lower_case = TRUE,
  uid = random_string("regex_tokenizer_"),
  ...
) {
  ml_process_feature(
    x = x,
    r_class = "ml_regex_tokenizer",
    uid = uid,
    stage_constructor = new_ml_regex_tokenizer,
    invoke_steps = list(
      input_col = input_col,
      output_col = output_col,
      gaps = gaps,
      min_token_length = min_token_length,
      pattern = pattern,
      to_lower_case = to_lower_case
    )
  )
}

#' @export
ft_regex_tokenizer.spark_connection <- ft_regex_tokenizer_impl

#' @export
ft_regex_tokenizer.ml_pipeline <- ft_regex_tokenizer_impl

#' @export
ft_regex_tokenizer.tbl_spark <- ft_regex_tokenizer_impl

new_ml_regex_tokenizer <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_regex_tokenizer")
}
