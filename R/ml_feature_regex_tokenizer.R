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
ft_regex_tokenizer <- function(x, input_col = NULL, output_col = NULL, gaps = TRUE,
                               min_token_length = 1, pattern = "\\s+", to_lower_case = TRUE,
                               uid = random_string("regex_tokenizer_"), ...) {
  check_dots_used()
  UseMethod("ft_regex_tokenizer")
}

ml_regex_tokenizer <- ft_regex_tokenizer

ft_regex_tokenizer_impl <- function(x, input_col = NULL, output_col = NULL, gaps = TRUE,
                                    min_token_length = 1, pattern = "\\s+", to_lower_case = TRUE,
                                    uid = random_string("regex_tokenizer_"), ...) {
  ft_process_step(
    x = x,
    uid = uid,
    r_class = "ml_regex_tokenizer",
    step_class = "ml_regex_tokenizer",
    invoke_steps = list(
      gaps = cast_scalar_logical(gaps),
      min_token_length = cast_scalar_integer(min_token_length),
      pattern = cast_string(pattern),
      to_lower_case = cast_scalar_logical(to_lower_case),
      input_col = cast_nullable_string(input_col),
      output_col = cast_nullable_string(output_col)
    )
  )
}

#' @export
ft_regex_tokenizer.spark_connection <- ft_regex_tokenizer_impl

#' @export
ft_regex_tokenizer.ml_pipeline <- ft_regex_tokenizer_impl

#' @export
ft_regex_tokenizer.tbl_spark <- ft_regex_tokenizer_impl
