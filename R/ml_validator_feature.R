input_output_mapping <- list(
  input.col = "input_col",
  output.col = "output_col"
)

ml_validator_hashing_tf <- function(args, nms) {
  ml_extract_specified_args(
    within(args, {
      ensure_scalar_boolean(binary)
      num_features <- ensure_scalar_integer(num_features)
    }),
    nms
  )
}

ml_validator_binarizer <- function(args, nms) {
  ml_extract_specified_args(
    within(args, {
      threshold <- ensure_scalar_double(threshold)
    }),
    nms, input_output_mapping
  )
}

ml_validator_string_indexer <- function(args, nms) {
  ml_extract_specified_args(
    within(args, {
      handle_invalid <- rlang::arg_match(handle_invalid, c("error", "skip", "keep"))
    }),
    nms, input_output_mapping
  )
}

ml_validator_one_hot_encoder <- function(args, nms) {
  ml_extract_specified_args(
    within(args, {
      drop_last <- ensure_scalar_boolean(drop_last)
    }),
    nms,
    c(input_output_mapping, list(drop.last = "drop_last"))
  )
}

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
