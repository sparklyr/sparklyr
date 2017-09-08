ml_validator_hashing_tf <- function(args, current_args) {
  within(ml_args_to_validate(args, current_args), {
    ensure_scalar_boolean(binary)
    num_features <- ensure_scalar_integer(num_features)
  }) %>%
    `[`(names(args))
}

ml_validator_binarizer <- function(args, current_args) {
  within(ml_args_to_validate(args, current_args), {
    threshold <- ensure_scalar_double(threshold)
  }) %>%
    `[`(names(args))
}
