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

ml_validate_decision_tree_args <- function(.args) {
  .args <- ml_backwards_compatibility(.args, list(
    max.bins = "max_bins",
    max.depth = "max_depth",
    min.info.gain = "min_info_gain",
    min.rows = "min_instances_per_node",
    checkpoint.interval = "checkpoint_interval",
    cache.node.ids = "cache_node_ids",
    max.memory = "max_memory_in_mb"
  ))
  .args[["max_bins"]] <- cast_scalar_integer(.args[["max_bins"]])
  .args[["max_depth"]] <- cast_scalar_integer(.args[["max_depth"]])
  .args[["min_info_gain"]] <- cast_scalar_double(.args[["min_info_gain"]])
  .args[["min_instances_per_node"]] <- cast_scalar_integer(.args[["min_instances_per_node"]])
  .args[["seed"]] <- cast_nullable_scalar_integer(.args[["seed"]])
  .args[["checkpoint_interval"]] <- cast_scalar_integer(.args[["checkpoint_interval"]])
  .args[["cache_node_ids"]] <- cast_scalar_logical(.args[["cache_node_ids"]])
  .args[["max_memory_in_mb"]] <- cast_scalar_integer(.args[["max_memory_in_mb"]])
  .args
}

validate_no_formula <- function(.args) {
  if (!is.null(.args[["formula"]])) stop("`formula` may only be specified when `x` is a `tbl_spark`.")
  .args
}

validate_args_predictor <- function(.args) {
  .args <- validate_no_formula(.args)
  .args[["features_col"]] <- cast_string(.args[["features_col"]])
  .args[["label_col"]] <- cast_string(.args[["label_col"]])
  .args[["prediction_col"]] <- cast_string(.args[["prediction_col"]])
  .args
}

validate_args_classifier <- function(.args) {
  .args <- validate_args_predictor(.args)
  .args[["probability_col"]] <- cast_string(.args[["probability_col"]])
  .args[["raw_prediction_col"]] <- cast_string(.args[["raw_prediction_col"]])
  .args
}
