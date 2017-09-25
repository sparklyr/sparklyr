input_output_mapping <- list(
  input.col = "input_col",
  output.col = "output_col"
)

# HashingTF
ml_validator_hashing_tf <- function(args, nms) {
  ml_extract_specified_args(
    within(args, {
      ensure_scalar_boolean(binary)
      num_features <- ensure_scalar_integer(num_features)
    }),
    nms
  )
}

# Binarizer
ml_validator_binarizer <- function(args, nms) {
  ml_extract_specified_args(
    within(args, {
      threshold <- ensure_scalar_double(threshold)
    }),
    nms, input_output_mapping
  )
}

# StringIndexer
ml_validator_string_indexer <- function(args, nms) {
  ml_extract_specified_args(
    within(args, {
      handle_invalid <- rlang::arg_match(handle_invalid, c("error", "skip", "keep"))
    }),
    nms, input_output_mapping
  )
}

# OneHotEncoder
ml_validator_one_hot_encoder <- function(args, nms) {
  ml_extract_specified_args(
    within(args, {
      drop_last <- ensure_scalar_boolean(drop_last)
    }),
    nms,
    c(input_output_mapping, list(drop.last = "drop_last"))
  )
}

# VectorAssembler
ml_validator_vector_assembler <- function(args, nms) {
  old_new_mapping <- list(
    input.col = "input_cols",
    output.col = "output_col"
  )
  ml_extract_specified_args(
    within(args, {
      input_cols <- input_cols %>%
        lapply(ensure_scalar_character)
      output_col <- ensure_scalar_character(output_col)
    }),
    nms, old_new_mapping
  )
}

# Tokenizer
ml_validator_tokenizer <- function(args, nms) {
  ml_extract_specified_args(
    args, nms, input_output_mapping
  )
}

# DCT
ml_validator_dct <- function(args, nms) {
  ml_extract_specified_args(
    within(args, {
      inverse <- ensure_scalar_boolean(inverse)
    }), nms, input_output_mapping
  )
}

# IndexToString
ml_validator_index_to_string <- function(args, nms) {
  ml_extract_specified_args(
    within(args, {
      if (!rlang::is_null(labels))
        labels <- lapply(labels, ensure_scalar_character)
    }), nms, input_output_mapping
  )
}

# Bucketizer
ml_validator_bucketizer <- function(args, nms) {
  ml_extract_specified_args(
    within(args, {
      if (length(splits) < 3) stop("length(splits) must be at least 3")
      splits <- lapply(splits, ensure_scalar_double)
      handle_invalid <- rlang::arg_match(handle_invalid, c("error", "skip", "keep"))
    }), nms, input_output_mapping
  )
}

# ElementwiseProduct
ml_validator_elementwise_product <- function(args, nms) {
  ml_extract_specified_args(
    within(args, {
      scaling_vec <- lapply(scaling_vec, ensure_scalar_double)
    }), nms, input_output_mapping
  )
}

# RegexTokenizer
ml_validator_regex_tokenizer <- function(args, nms) {
  ml_extract_specified_args(
    within(args, {
      gaps <- ensure_scalar_boolean(gaps)
      min_token_length <- ensure_scalar_integer(min_token_length)
      pattern <- ensure_scalar_character(pattern)
      to_lower_case <- ensure_scalar_boolean(to_lower_case)
    }), nms, input_output_mapping
  )
}
