ml_get_stage_validator <- function(x) {
  validator_mapping[[x]] %||% stop("validator mapping failed")
}

ml_args_to_validate <- function(args, current_args) {
  input_arg_names <- names(args)
  current_arg_names <- names(current_args) %>%
    setdiff(input_arg_names)
  c(current_args[current_arg_names], args)
}


ml_validate_args <- function(env = rlang::caller_env(2)) {
  constructor_frame <- rlang::caller_frame()
  validator_fn <- constructor_frame$fn_name %>%
    (function(x) gsub("^ml_", "ml_validator_", x)) %>%
    (function(x) gsub("\\..*$", "", x))
  args <- constructor_frame$expr %>%
    rlang::lang_standardise() %>%
    rlang::lang_args() %>%
    lapply(rlang::eval_tidy, env = env)
  default_args <- Filter(Negate(rlang::is_symbol), # filter out args without defaults
                         rlang::fn_fmls(constructor_frame$fn)) %>%
    lapply(rlang::eval_tidy, env = env)

  validated_args <- rlang::invoke(
    validator_fn, args = args, current_args = default_args
  )

  invisible(
    lapply(names(validated_args),
           function(x) assign(x, validated_args[[x]], constructor_frame$env))
  )
}
