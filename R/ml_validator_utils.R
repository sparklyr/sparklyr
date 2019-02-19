ml_map_class <- function(x) {
  rlang::env_get(.globals$ml_class_mapping, x, default = NULL, inherit = TRUE)
}

ml_map_package <- function(x) {
  rlang::env_get(.globals$ml_package_mapping, x, default = NULL, inherit = TRUE)
}

ml_get_stage_validator <- function(jobj) {
  cl <- jobj_class(jobj, simple_name = FALSE)[[1]]
  get(
    paste0("validator_", ml_map_class(cl)),
    envir = asNamespace(ml_map_package(cl)),
    mode = "function"
  )
}

ml_get_stage_constructor <- function(jobj) {
  cl <- jobj_class(jobj, simple_name = FALSE)[[1]]
  get(
    ml_map_class(cl),
    envir = asNamespace(ml_map_package(cl)),
    mode = "function"
  )
}

#' Standardize Formula Input for `ml_model`
#'
#' Generates a formula string from user inputs, to be used in `ml_model` constructor.
#'
#' @param formula The `formula` argument.
#' @param response The `response` argument.
#' @param features The `features` argument.
#' @export
#' @keywords internal
ml_standardize_formula <- function(formula = NULL, response = NULL, features = NULL) {
  if (is.null(formula) && !is.null(response)) {
    # if 'formula' isn't specified but 'response' is...
    if (rlang::is_formula(response)) {
      # if 'response' is a formula, warn if 'features' is also specified
      if (!is.null(features)) warning("'features' is ignored when a formula is specified")
      # convert formula to string
      rlang::expr_text(response, width = 500L)
    } else
      # otherwise, if both 'response' and 'features' are specified, treat them as
      #   variable names, and construct formula string
      paste0(response, " ~ ", paste(features, collapse = " + "))
  } else if (is.null(formula) && is.null(response) && !is.null(features)) {
    # if only 'features' is specified, e.g. in clustering algorithms
    paste0("~ ", paste(features, collapse = " + "))
  } else if (!is.null(formula)) {
    # now if 'formula' is specified, check to see that 'response' and 'features' are not
    if (!is.null(response) || !is.null(features))
      stop("only one of 'formula' or 'response'-'features' should be specified")
    if (rlang::is_formula(formula))
      # if user inputs a formula, convert it to string
      rlang::expr_text(formula, width = 500L)
    else
      # otherwise just returns as is
      formula
  } else
    formula
}

ml_validate_decision_tree_args <- function(.args) {
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
