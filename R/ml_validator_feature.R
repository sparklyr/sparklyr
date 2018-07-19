input_output_mapping <- list(
  input.col = "input_col",
  output.col = "output_col"
)

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

