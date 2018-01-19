ml_map_class <- function(x) {
  ml_class_mapping[[x]]
}

ml_get_stage_validator <- function(jobj) {
  paste0("ml_validator_",
         ml_map_class(jobj_class(jobj)[1]))
}

ml_get_stage_constructor <- function(jobj) {
  package <- jobj %>%
    jobj_class(simple_name = FALSE) %>%
    head(1) %>%
    strsplit("\\.") %>%
    rlang::flatten_chr() %>%
    dplyr::nth(-2)
  prefix <- if (identical(package, "feature")) "ft_" else "ml_"
  paste0(prefix, ml_map_class(jobj_class(jobj)[1]))
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


ml_ratify_args <- function(env = rlang::caller_env(2)) {
  caller_frame <- rlang::caller_frame()
  caller <- caller_frame$fn_name %>%
    strsplit("\\.") %>%
    unlist() %>%
    head(1)

  if (grepl("^ml_", caller)) {
    # if caller is a ml_ function (as opposed to ft_),
    #   get calls to function in the stack
    calls <- sys.calls()
    calls <- calls %>%
      sapply(`[[`, 1) %>%
      sapply(deparse) %>%
      grep(caller, ., value = TRUE)

    # if formula is specified and we didn't dispatch to ml_*.tbl_spark,
    #   throw error
    if (!any(grepl("tbl_spark", calls)) &&
        !rlang::is_null(caller_frame$env[["formula"]]))
      stop(paste0("formula should only be specified when calling ",
                  caller, " on a tbl_spark"))
  }

  validator_fn <- caller_frame$fn_name %>%
    gsub("^(ml_|ft_)", "ml_validator_", .) %>%
    gsub("\\..*$", "", .)
  args <- caller_frame %>%
    rlang::lang_standardise() %>%
    rlang::lang_args()

  default_args <- Filter(Negate(rlang::is_symbol),
                         rlang::fn_fmls(caller_frame$fn)) %>%
    lapply(rlang::new_quosure, env = caller_frame$env)

  args_to_validate <- ml_args_to_validate(args, default_args) %>%
    lapply(rlang::eval_tidy, env = env)

  validated_args <- rlang::invoke(
    validator_fn, args = args_to_validate, nms = names(args)
  )

  invisible(
    lapply(names(validated_args),
           function(x) assign(x, validated_args[[x]], caller_frame$env))
  )
}

ml_formula_transformation <- function(env = rlang::caller_env(2)) {
  caller_frame <- rlang::caller_frame()
  args <- caller_frame$expr %>%
    rlang::lang_standardise() %>%
    rlang::lang_args() %>%
    `[`(c("formula", "response", "features")) %>%
    lapply(rlang::eval_tidy, env = env)

  formula <- if (is.null(args$formula) && !is.null(args$response)) {
    # if 'formula' isn't specified but 'response' is...
    if (rlang::is_formula(args$response)) {
      # if 'response' is a formula, warn if 'features' is also specified
      if (!is.null(args$features)) warning("'features' is ignored when a formula is specified")
      # convert formula to string
      rlang::expr_text(args$response, width = 500L)
    } else
      # otherwise, if both 'response' and 'features' are specified, treat them as
      #   variable names, and construct formula string
      paste0(args$response, " ~ ", paste(args$features, collapse = " + "))
  } else if (is.null(args$formula) && is.null(args$response) && !is.null(args$features)) {
    # if only 'features' is specified, e.g. in clustering algorithms
    paste0("~ ", paste(args$features, collapse = " + "))
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

  assign("formula", formula, caller_frame$env)
}

ml_validate_args <- function(
  args, expr = NULL,
  mapping_list = list(
    input.col = "input_col",
    output.col = "output_col"
  )) {
  validations <- rlang::enexpr(expr)

  data <- names(args) %>%
    setdiff(mapping_list[intersect(names(mapping_list), .)]) %>%
    args[.] %>%
    rlang::set_names(
      mapply(`%||%`, mapping_list[names(.)], names(.))
    )

  rlang::invoke(within,
                data = data,
                expr = validations,
                .bury = NULL)
}

ml_extract_args <- function(
  args, nms, mapping_list = list(
    input.col = "input_col",
    output.col = "output_col"
  )) {
  args[mapply(`%||%`, mapping_list[nms], nms)]
}

ml_tree_param_mapping <- function() {
  list(
    max.bins = "max_bins",
    max.depth = "max_depth",
    min.info.gain = "min_info_gain",
    min.rows = "min_instances_per_node",
    checkpoint.interval = "checkpoint_interval",
    cache.node.ids = "cache_node_ids",
    max.memory = "max_memory_in_mb"
  )
}

ml_validate_decision_tree_args <- function(args) {
  args %>%
    ml_validate_args({
      max_bins <- ensure_scalar_integer(max_bins)
      max_depth <- ensure_scalar_integer(max_depth)
      min_info_gain <- ensure_scalar_double(min_info_gain)
      min_instances_per_node <- ensure_scalar_integer(min_instances_per_node)
      seed <- ensure_scalar_integer(seed, allow.null = TRUE)
      checkpoint_interval <- ensure_scalar_integer(checkpoint_interval)
      cache_node_ids <- ensure_scalar_boolean(cache_node_ids)
      max_memory_in_mb <- ensure_scalar_integer(max_memory_in_mb)
    }, ml_tree_param_mapping())
}
