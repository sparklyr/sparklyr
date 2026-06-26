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

#' @export
ft_tokenizer.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  uid = random_string("tokenizer_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_tokenizer()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.Tokenizer",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  )

  new_ml_tokenizer(jobj)
}

#' @export
ft_tokenizer.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  uid = random_string("tokenizer_"),
  ...
) {
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
ft_tokenizer.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  uid = random_string("tokenizer_"),
  ...
) {
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

#' @export
ft_regex_tokenizer.spark_connection <- function(
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
    x,
    "org.apache.spark.ml.feature.RegexTokenizer",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    invoke("setGaps", .args[["gaps"]]) %>%
    invoke("setMinTokenLength", .args[["min_token_length"]]) %>%
    invoke("setPattern", .args[["pattern"]]) %>%
    invoke("setToLowercase", .args[["to_lower_case"]])

  new_ml_regex_tokenizer(jobj)
}

#' @export
ft_regex_tokenizer.ml_pipeline <- function(
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
  stage <- ft_regex_tokenizer.spark_connection(
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
ft_regex_tokenizer.tbl_spark <- function(
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
  stage <- ft_regex_tokenizer.spark_connection(
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
  .args[["min_token_length"]] <- cast_scalar_integer(.args[[
    "min_token_length"
  ]])
  .args[["pattern"]] <- cast_string(.args[["pattern"]])
  .args[["to_lower_case"]] <- cast_scalar_logical(.args[["to_lower_case"]])
  .args
}

#' Default stop words
#'
#' Loads the default stop words for the given language.
#'
#' @param sc A \code{spark_connection}
#' @param language A character string.
#' @template roxlate-ml-dots
#'
#' @details Supported languages: danish, dutch, english, finnish, french,
#'   german, hungarian, italian, norwegian, portuguese, russian, spanish,
#'   swedish, turkish. Defaults to English. See \url{https://snowballstem.org/projects.html}
#'   for more details
#'
#' @return A list of stop words.
#'
#' @seealso \code{\link{ft_stop_words_remover}}
#' @export
ml_default_stop_words <- function(
  sc,
  language = c(
    "english",
    "danish",
    "dutch",
    "finnish",
    "french",
    "german",
    "hungarian",
    "italian",
    "norwegian",
    "portuguese",
    "russian",
    "spanish",
    "swedish",
    "turkish"
  ),
  ...
) {
  language <- rlang::arg_match(language)
  invoke_static(
    sc,
    "org.apache.spark.ml.feature.StopWordsRemover",
    "loadDefaultStopWords",
    language
  )
}

#' Feature Transformation -- StopWordsRemover (Transformer)
#'
#' A feature transformer that filters out stop words from input.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param case_sensitive Whether to do a case sensitive comparison over the stop words.
#' @param stop_words The words to be filtered out.
#'
#' @seealso \code{\link{ml_default_stop_words}}
#'
#' @export
ft_stop_words_remover <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  case_sensitive = FALSE,
  stop_words = ml_default_stop_words(spark_connection(x), "english"),
  uid = random_string("stop_words_remover_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_stop_words_remover")
}

ml_stop_words_remover <- ft_stop_words_remover

#' @export
ft_stop_words_remover.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  case_sensitive = FALSE,
  stop_words = ml_default_stop_words(spark_connection(x), "english"),
  uid = random_string("stop_words_remover_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    case_sensitive = case_sensitive,
    stop_words = stop_words,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_stop_words_remover()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.StopWordsRemover",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    invoke("setCaseSensitive", .args[["case_sensitive"]]) %>%
    invoke("setStopWords", .args[["stop_words"]])

  new_ml_stop_words_remover(jobj)
}

#' @export
ft_stop_words_remover.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  case_sensitive = FALSE,
  stop_words = ml_default_stop_words(spark_connection(x), "english"),
  uid = random_string("stop_words_remover_"),
  ...
) {
  stage <- ft_stop_words_remover.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    case_sensitive = case_sensitive,
    stop_words = stop_words,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_stop_words_remover.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  case_sensitive = FALSE,
  stop_words = ml_default_stop_words(spark_connection(x), "english"),
  uid = random_string("stop_words_remover_"),
  ...
) {
  stage <- ft_stop_words_remover.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    case_sensitive = case_sensitive,
    stop_words = stop_words,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_stop_words_remover <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_stop_words_remover")
}

validator_ml_stop_words_remover <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["case_sensitive"]] <- cast_scalar_logical(.args[["case_sensitive"]])
  .args[["stop_words"]] <- cast_string_list(.args[["stop_words"]])
  .args
}

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
ft_ngram <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  n = 2,
  uid = random_string("ngram_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_ngram")
}

ml_ngram <- ft_ngram

#' @export
ft_ngram.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  n = 2,
  uid = random_string("ngram_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    n = n,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_ngram()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.NGram",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    invoke("setN", .args[["n"]])

  new_ml_ngram(jobj)
}

#' @export
ft_ngram.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  n = 2,
  uid = random_string("ngram_"),
  ...
) {
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
ft_ngram.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  n = 2,
  uid = random_string("ngram_"),
  ...
) {
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
