ml_validator_hashing_tf <- function(args, current_args) {
  within(ml_args_to_validate(args, current_args), {
    ensure_scalar_boolean(binary)
    num_features <- ensure_scalar_integer(num_features)
  })
}

ml_validator_binarizer <- function(args, current_args) {
  within(ml_args_to_validate(args, current_args), {
    threshold <- ensure_scalar_double(threshold)
  })
}

ml_validator_string_indexer <- function(args, current_args) {
  within(ml_args_to_validate(args, current_args), {
    handle_invalid <- rlang::arg_match(handle_invalid, c("error", "skip", "keep"))
  })
}

ml_validator_one_hot_encoder <- function(args, current_args) {
  within(ml_args_to_validate(args, current_args), {
    drop_last <- ensure_scalar_boolean(drop_last)
  })
}
