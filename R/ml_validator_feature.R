input_output_mapping <- list(
  input.col = "input_col",
  output.col = "output_col"
)

# HashingTF
ml_validator_hashing_tf <- function(args, nms) {
  ml_apply_validation(
    {
      ensure_scalar_boolean(binary)
      num_features <- ensure_scalar_integer(num_features)
    },
    args, nms, input_output_mapping
  )
}

# Binarizer
ml_validator_binarizer <- function(args, nms) {
  ml_apply_validation(
    {
      threshold <- ensure_scalar_double(threshold)
    },
    args, nms, input_output_mapping
  )
}

# StringIndexer
ml_validator_string_indexer <- function(args, nms) {
  ml_apply_validation(
    {
      handle_invalid <- rlang::arg_match(handle_invalid, c("error", "skip", "keep"))
    },
    args, nms, input_output_mapping
  )
}

# OneHotEncoder
ml_validator_one_hot_encoder <- function(args, nms) {
  ml_apply_validation(
    {
      drop_last <- ensure_scalar_boolean(drop_last)
    },
    args, nms, c(input_output_mapping, list(drop.last = "drop_last"))
  )
}

# VectorAssembler
ml_validator_vector_assembler <- function(args, nms) {
  old_new_mapping <- list(
    input.col = "input_cols",
    output.col = "output_col"
  )

  ml_apply_validation(
    {
      input_cols <- input_cols %>%
        lapply(ensure_scalar_character)
      output_col <- ensure_scalar_character(output_col)
    },
    args, nms, old_new_mapping
  )
}

# Tokenizer
ml_validator_tokenizer <- function(args, nms) {
  ml_apply_validation(
    {},
    args, nms, input_output_mapping
  )
}

# DCT
ml_validator_dct <- function(args, nms) {
  ml_apply_validation(
    {
      inverse <- ensure_scalar_boolean(inverse)
    },
    args, nms, input_output_mapping
  )
}

# IndexToString
ml_validator_index_to_string <- function(args, nms) {
  ml_apply_validation(
    {
      if (!rlang::is_null(labels))
        labels <- lapply(labels, ensure_scalar_character)
    },
    args, nms, input_output_mapping
  )
}

# Bucketizer
ml_validator_bucketizer <- function(args, nms) {
  ml_apply_validation(
    {
      if (length(splits) < 3) stop("length(splits) must be at least 3")
      splits <- lapply(splits, ensure_scalar_double)
      handle_invalid <- rlang::arg_match(handle_invalid, c("error", "skip", "keep"))
    },
    args, nms, input_output_mapping
  )
}

# ElementwiseProduct
ml_validator_elementwise_product <- function(args, nms) {
  ml_apply_validation(
    {
      scaling_vec <- lapply(scaling_vec, ensure_scalar_double)
    },
    args, nms, input_output_mapping
  )
}

# RegexTokenizer
ml_validator_regex_tokenizer <- function(args, nms) {
  ml_apply_validation(
    {
      gaps <- ensure_scalar_boolean(gaps)
      min_token_length <- ensure_scalar_integer(min_token_length)
      pattern <- ensure_scalar_character(pattern)
      to_lower_case <- ensure_scalar_boolean(to_lower_case)
    },
    args, nms, input_output_mapping
  )
}

# StopWordsRemover
ml_validator_stop_words_remover <- function(args, nms) {
  ml_apply_validation(
    {
      case_sensitive <- ensure_scalar_boolean(case_sensitive)
      stop_words <- lapply(stop_words, ensure_scalar_character)
    },
    args, nms, input_output_mapping
  )
}

# CountVectorizer

ml_validator_count_vectorizer <- function(args, nms) {
  old_new_mapping <- c(
    list(
      min.df = "min_df",
      min.tf = "min_tf",
      vocab.size = "vocab_size"
    ), input_output_mapping
  )

  ml_apply_validation(
    {
      binary <- ensure_scalar_boolean(binary)
      min_df <- ensure_scalar_double(min_df)
      min_tf <- ensure_scalar_double(min_tf)
      vocab_size <- ensure_scalar_integer(vocab_size)
    },
    args, nms, old_new_mapping
  )
}

# QuantileDiscretizer
ml_validator_quantile_discretizer <- function(args, nms) {
  old_new_mapping <- c(
    list(
      n.buckets = "num_buckets"
    ), input_output_mapping
  )

  ml_apply_validation(
    {
      handle_invalid <- rlang::arg_match(handle_invalid, c("error", "skip", "keep"))
      num_buckets <- ensure_scalar_integer(num_buckets)
      relative_error <- ensure_scalar_double(relative_error)
    },
    args, nms, old_new_mapping
  )
}

# SQLTransformer
ml_validator_sql_transformer <- function(args, nms) {
  ml_apply_validation(
    {
      statement <- ensure_scalar_character(statement)
    },
    args, nms, list(sql = "statement")
  )
}

