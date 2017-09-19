ml_map_class <- function(x) {
  ml_class_mapping[[x]] %||% stop("class mapping failed")
}

ml_get_stage_validator <- function(x) {
  paste0("ml_validator_", ml_map_class(x))
}

ml_get_stage_constructor <- function(x) {
  paste0("ml_", ml_map_class(x))
}

ml_args_to_validate <- function(args, current_args, default_args = current_args) {
  # creates a list of arguments to validate
  # precedence: user input, then, current pipeline params,
  #   then default args from formals
  input_arg_names <- names(args)
  current_arg_names <- names(current_args)
  default_arg_names <- names(default_args)

  args %>%
    c(current_args[setdiff(current_arg_names, input_arg_names)]) %>%
    c(default_args[setdiff(default_arg_names,
                           union(input_arg_names, current_arg_names)
                           )]
      )
}


ml_validate_args <- function(env) {
  constructor_frame <- rlang::caller_frame()
  validator_fn <- constructor_frame$fn_name %>%
    (function(x) gsub("^ml_", "ml_validator_", x)) %>%
    (function(x) gsub("\\..*$", "", x))
  args <- constructor_frame$expr %>%
    rlang::lang_standardise() %>%
    rlang::lang_args() %>%
    # evaluate in calling environment of public function
    lapply(rlang::eval_tidy, env = env)
  # filter out args without defaults
  default_args <- Filter(Negate(rlang::is_symbol),
                         rlang::fn_fmls(constructor_frame$fn)) %>%
    # evaluate default args in package namespace
    lapply(rlang::eval_tidy, env = rlang::ns_env("sparklyr"))

  args_to_validate <- ml_args_to_validate(args, default_args)

  validated_args <- rlang::invoke(
    validator_fn, args = args_to_validate
  ) %>%
    `[`(names(args))

  invisible(
    lapply(names(validated_args),
           function(x) assign(x, validated_args[[x]], constructor_frame$env))
  )
}

ml_formula_transformation <- function(env = rlang::caller_env(2)) {
  constructor_frame <- rlang::caller_frame()
  args <- constructor_frame$expr %>%
    rlang::lang_standardise() %>%
    rlang::lang_args() %>%
    `[`(c("formula", "response", "features")) %>%
    lapply(rlang::eval_tidy, env = env)

  formula <- if (is.null(args$formula) && !is.null(args$response)) {
    # if 'formula' isn't specified but 'response' is...
    if (rlang::is_formula(args$response)) {
      # if 'response' is a formula, warn is 'features' is also specified
      if (!is.null(args$features)) warning("'features' is ignored when a formula is specified")
      # convert formula to string
      rlang::expr_text(args$response, width = 500L)
    } else
    # otherwise, if both 'response' and 'features' are specified, treat them as
    #   variable names, and construct formula string
      paste0(args$response, " ~ ", paste(args$features, collapse = " + "))
  } else if (!is.null(args$formula)) {
    # now if 'formula' is specified, check to see that 'response' and 'features' are not
    if (!is.null(args$response) || !is.null(args$features))
      stop("only one of 'formula' or 'response'-'features' should be specified")
    if (rlang::is_formula(args$formula))
      # if user inputs a formula, convert it to string
      rlang::expr_text(args$formula, width = 500L)
    else
      # otherwise just returns as is
      args$formula
  } else
    args$formula

  assign("formula", formula, constructor_frame$env)
}
