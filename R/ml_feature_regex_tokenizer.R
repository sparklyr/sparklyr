#' Feature Tranformation -- RegexTokenizer (Transformer)
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
  x, input_col, output_col, gaps = TRUE,
  min_token_length = 1L, pattern = "\\s+", to_lower_case = TRUE,
  uid = random_string("regex_tokenizer_"), ...) {
  UseMethod("ft_regex_tokenizer")
}

#' @export
ft_regex_tokenizer.spark_connection <- function(
  x, input_col, output_col, gaps = TRUE,
  min_token_length = 1L, pattern = "\\s+", to_lower_case = TRUE,
  uid = random_string("regex_tokenizer_"), ...) {

  ml_ratify_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.RegexTokenizer",
                             input_col, output_col, uid) %>%
    invoke("setGaps", gaps) %>%
    invoke("setMinTokenLength", min_token_length) %>%
    invoke("setPattern", pattern) %>%
    invoke("setToLowercase", to_lower_case)

  new_ml_regex_tokenizer(jobj)
}

#' @export
ft_regex_tokenizer.ml_pipeline <- function(
  x, input_col, output_col, gaps = TRUE,
  min_token_length = 1L, pattern = "\\s+", to_lower_case = TRUE,
  uid = random_string("regex_tokenizer_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_regex_tokenizer.tbl_spark <- function(
  x, input_col, output_col, gaps = TRUE,
  min_token_length = 1L, pattern = "\\s+", to_lower_case = TRUE,
  uid = random_string("regex_tokenizer_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_regex_tokenizer <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_regex_tokenizer")
}
