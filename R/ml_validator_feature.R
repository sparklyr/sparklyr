input_output_mapping <- list(
  input.col = "input_col",
  output.col = "output_col"
)



# OneHotEncoder
ml_validator_one_hot_encoder <- function(args, nms) {
  old_new_mapping <- c(input_output_mapping, list(drop.last = "drop_last"))
  args %>%
    ml_validate_args({
      drop_last <- ensure_scalar_boolean(drop_last)
    }) %>%
    ml_extract_args(nms)
}

# VectorAssembler
ml_validator_vector_assembler <- function(args, nms) {
  old_new_mapping <- list(
    input.col = "input_cols",
    output.col = "output_col"
  )

  args %>%
    ml_validate_args({
      input_cols <- input_cols %>%
        lapply(ensure_scalar_character)
      output_col <- ensure_scalar_character(output_col)
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Tokenizer
ml_validator_tokenizer <- function(args, nms) {
  args %>%
    ml_validate_args() %>%
    ml_extract_args(nms)
}

# IndexToString
ml_validator_index_to_string <- function(args, nms) {
  args %>%
    ml_validate_args({
      if (!rlang::is_null(labels))
        labels <- lapply(labels, ensure_scalar_character)
    }) %>%
    ml_extract_args(nms)
}

# RegexTokenizer
ml_validator_regex_tokenizer <- function(args, nms) {
  args %>%
    ml_validate_args(
    {
      gaps <- ensure_scalar_boolean(gaps)
      min_token_length <- ensure_scalar_integer(min_token_length)
      pattern <- ensure_scalar_character(pattern)
      to_lower_case <- ensure_scalar_boolean(to_lower_case)
    }) %>%
    ml_extract_args(nms)
}

# StopWordsRemover
ml_validator_stop_words_remover <- function(args, nms) {
  args %>%
    ml_validate_args(
    {
      case_sensitive <- ensure_scalar_boolean(case_sensitive)
      stop_words <- lapply(stop_words, ensure_scalar_character)
    }) %>%
    ml_extract_args(nms)
}

# SQLTransformer
ml_validator_sql_transformer <- function(args, nms) {
  args %>%
    ml_validate_args({
      statement <- ensure_scalar_character(statement)
    }, list(sql = "statement")) %>%
    ml_extract_args(nms, list(sql = "statement"))
}

