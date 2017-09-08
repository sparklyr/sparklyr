ml_get_stage_validator <- function(x) {
  validator_mapping[[x]] %||% stop("validator mapping failed")
}

ml_args_to_validate <- function(args, current_args) {
  input_arg_names <- names(args)
  current_arg_names <- names(current_args) %>%
    setdiff(input_arg_names)
  c(current_args[current_arg_names], args)
}
