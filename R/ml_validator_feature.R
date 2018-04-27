input_output_mapping <- list(
  input.col = "input_col",
  output.col = "output_col"
)

# HashingTF
ml_validator_hashing_tf <- function(args, nms) {
  args %>%
    ml_validate_args({
      binary <- ensure_scalar_boolean(binary)
      num_features <- ensure_scalar_integer(num_features)
    }) %>%
    ml_extract_args(nms)
}

# Binarizer
ml_validator_binarizer <- function(args, nms) {
  args %>%
    ml_validate_args({
      threshold <- ensure_scalar_double(threshold)
    }) %>%
    ml_extract_args(nms)
}

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

# DCT
ml_validator_dct <- function(args, nms) {
  args %>%
    ml_validate_args({
      inverse <- ensure_scalar_boolean(inverse)
    }) %>%
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


# ElementwiseProduct
ml_validator_elementwise_product <- function(args, nms) {
  args %>%
    ml_validate_args({
      scaling_vec <- lapply(scaling_vec, ensure_scalar_double)
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

