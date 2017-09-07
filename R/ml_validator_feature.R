#' @export
ml_validator_hashing_tf <- function(.args, .current_args) {
  input_arg_names <- names(.args)
  current_arg_names <- names(.current_args) %>%
    setdiff(input_arg_names)
  .args <- c(.current_args[current_arg_names], .args)
  ensure_scalar_boolean(.args$binary)
  .args$num_features <- ensure_scalar_integer(.args$num_features)
  .args[input_arg_names]
}
