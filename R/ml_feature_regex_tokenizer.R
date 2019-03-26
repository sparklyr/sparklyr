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

#' @export
ft_regex_tokenizer.spark_connection <- function(x, input_col = NULL, output_col = NULL, gaps = TRUE,
                                                min_token_length = 1, pattern = "\\s+", to_lower_case = TRUE,
                                                uid = random_string("regex_tokenizer_"), ...) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    gaps = gaps,
    min_token_length = min_token_length,
    pattern = pattern,
    to_lower_case = to_lower_case,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_regex_tokenizer()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.RegexTokenizer",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]) %>%
    invoke("setGaps", .args[["gaps"]]) %>%
    invoke("setMinTokenLength", .args[["min_token_length"]]) %>%
    invoke("setPattern", .args[["pattern"]]) %>%
    invoke("setToLowercase", .args[["to_lower_case"]])

  new_ml_regex_tokenizer(jobj)
}

#' @export
ft_regex_tokenizer.ml_pipeline <- function(x, input_col = NULL, output_col = NULL, gaps = TRUE,
                                           min_token_length = 1, pattern = "\\s+", to_lower_case = TRUE,
                                           uid = random_string("regex_tokenizer_"), ...) {
  stage <-ft_regex_tokenizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    gaps = gaps,
    min_token_length = min_token_length,
    pattern = pattern,
    to_lower_case = to_lower_case,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_regex_tokenizer.tbl_spark <- function(x, input_col = NULL, output_col = NULL, gaps = TRUE,
                                         min_token_length = 1, pattern = "\\s+", to_lower_case = TRUE,
                                         uid = random_string("regex_tokenizer_"), ...) {
  stage <-ft_regex_tokenizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    gaps = gaps,
    min_token_length = min_token_length,
    pattern = pattern,
    to_lower_case = to_lower_case,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_regex_tokenizer <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_regex_tokenizer")
}

validator_ml_regex_tokenizer <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["gaps"]] <- cast_scalar_logical(.args[["gaps"]])
  .args[["min_token_length"]] <- cast_scalar_integer(.args[["min_token_length"]])
  .args[["pattern"]] <- cast_string(.args[["pattern"]])
  .args[["to_lower_case"]] <- cast_scalar_logical(.args[["to_lower_case"]])
  .args
}
