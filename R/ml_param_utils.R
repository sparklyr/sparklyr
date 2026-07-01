#' Spark ML -- ML Params
#'
#' Helper methods for working with parameters for ML objects.
#'
#' @param x A Spark ML object, either a pipeline stage or an evaluator.
#' @param param The parameter to extract or set.
#' @param params A vector of parameters to extract.
#' @param allow_null Whether to allow \code{NULL} results when extracting parameters. If \code{FALSE}, an error will be thrown if the specified parameter is not found. Defaults to \code{FALSE}.
#' @template roxlate-ml-dots
#' @name ml-params
NULL

#' @rdname ml-params
#' @export
ml_is_set <- function(x, param, ...) {
  UseMethod("ml_is_set")
}

#' @export
ml_is_set.ml_pipeline_stage <- function(x, param, ...) {
  jobj <- spark_jobj(x)
  param_jobj <- jobj %>%
    invoke(ml_map_param_names(param, direction = "rs"))
  jobj %>%
    invoke("isSet", param_jobj)
}

#' @export
ml_is_set.spark_jobj <- function(x, param, ...) {
  param_jobj <- x %>%
    invoke(ml_map_param_names(param, direction = "rs"))
  x %>%
    invoke("isSet", param_jobj)
}

#' @rdname ml-params
#' @export
ml_param_map <- function(x, ...) {
  x$param_map %||% stop("'x' does not have a param map")
}

#' @rdname ml-params
#' @export
ml_param <- function(x, param, allow_null = FALSE, ...) {
  ml_param_map(x)[[param]] %||%
    (if (allow_null) NULL else stop("param ", param, " not found"))
}

#' @rdname ml-params
#' @export
ml_params <- function(x, params = NULL, allow_null = FALSE, ...) {
  params <- params %||% names(x$param_map)
  params %>%
    lapply(function(param) ml_param(x, param, allow_null)) %>%
    rlang::set_names(unlist(params))
}

ml_set_param <- function(x, param, value, ...) {
  setter <- param %>%
    ml_map_param_names(direction = "rs") %>%
    {
      paste0(
        "set",
        toupper(substr(., 1, 1)),
        substr(., 2, nchar(.))
      )
    }
  spark_jobj(x) %>%
    invoke(setter, value) %>%
    ml_call_constructor()
}

ml_get_param_map <- function(jobj) {
  sc <- spark_connection(jobj)

  invoke_static(
    sc,
    "sparklyr.MLUtils2",
    "getParamMap",
    jobj
  ) %>%
    ml_map_param_list_names()
}

ml_map_param_list_names <- function(x, direction = c("sr", "rs"), ...) {
  direction <- rlang::arg_match(direction)
  mapping <- if (identical(direction, "sr")) {
    genv_get_param_mapping_s_to_r()
  } else {
    genv_get_param_mapping_r_to_s()
  }

  rlang::set_names(
    x,
    unname(
      sapply(
        names(x),
        function(nm) {
          rlang::env_get(mapping, nm, default = NULL, inherit = TRUE) %||% nm
        }
      )
    )
  )
}

ml_map_param_names <- function(x, direction = c("sr", "rs"), ...) {
  direction <- rlang::arg_match(direction)
  mapping <- if (identical(direction, "sr")) {
    genv_get_param_mapping_s_to_r()
  } else {
    genv_get_param_mapping_r_to_s()
  }

  unname(
    sapply(
      x,
      function(nm) {
        rlang::env_get(mapping, nm, default = NULL, inherit = TRUE) %||% nm
      }
    )
  )
}

ml_map_class <- function(x) {
  rlang::env_get(genv_get_ml_class_mapping(), x, default = NULL, inherit = TRUE)
}

ml_map_package <- function(x) {
  rlang::env_get(
    genv_get_ml_package_mapping(),
    x,
    default = NULL,
    inherit = TRUE
  )
}

ml_get_stage_validator <- function(jobj) {
  cl <- jobj_class(jobj, simple_name = FALSE)[[1]]

  cl_fn <- paste0("validator_", ml_map_class(cl))
  pkg_env <- asNamespace(ml_map_package(cl))

  if (cl_fn %in% ls(pkg_env)) {
    get(
      cl_fn,
      envir = pkg_env,
      mode = "function"
    )
  } else {
    NULL
  }
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
ml_standardize_formula <- function(
  formula = NULL,
  response = NULL,
  features = NULL
) {
  if (is.null(formula) && !is.null(response)) {
    # if 'formula' isn't specified but 'response' is...
    if (rlang::is_formula(response)) {
      # if 'response' is a formula, warn if 'features' is also specified
      if (!is.null(features)) {
        warning("'features' is ignored when a formula is specified")
      }
      # convert formula to string
      rlang::expr_text(response, width = 500L)
    } else {
      # otherwise, if both 'response' and 'features' are specified, treat them as
      #   variable names, and construct formula string
      paste0(response, " ~ ", paste(features, collapse = " + "))
    }
  } else if (is.null(formula) && is.null(response) && !is.null(features)) {
    # if only 'features' is specified, e.g. in clustering algorithms
    paste0("~ ", paste(features, collapse = " + "))
  } else if (!is.null(formula)) {
    # now if 'formula' is specified, check to see that 'response' and 'features' are not
    if (!is.null(response) || !is.null(features)) {
      stop("only one of 'formula' or 'response'-'features' should be specified")
    }
    if (rlang::is_formula(formula)) {
      # if user inputs a formula, convert it to string
      rlang::expr_text(formula, width = 500L)
    } else {
      # otherwise just returns as is
      formula
    }
  } else {
    formula
  }
}

ml_validate_decision_tree_args <- function(.args) {
  .args[["max_bins"]] <- cast_scalar_integer(.args[["max_bins"]])
  .args[["max_depth"]] <- cast_scalar_integer(.args[["max_depth"]])
  .args[["min_info_gain"]] <- cast_scalar_double(.args[["min_info_gain"]])
  .args[["min_instances_per_node"]] <- cast_scalar_integer(.args[[
    "min_instances_per_node"
  ]])
  .args[["seed"]] <- cast_nullable_scalar_integer(.args[["seed"]])
  .args[["checkpoint_interval"]] <- cast_scalar_integer(.args[[
    "checkpoint_interval"
  ]])
  .args[["cache_node_ids"]] <- cast_scalar_logical(.args[["cache_node_ids"]])
  .args[["max_memory_in_mb"]] <- cast_scalar_integer(.args[[
    "max_memory_in_mb"
  ]])
  .args
}

validate_no_formula <- function(.args) {
  if (!is.null(.args[["formula"]])) {
    stop("`formula` may only be specified when `x` is a `tbl_spark`.")
  }
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

find_in_extensions <- function(what) {
  # Get package namespaces for sparkly and extensions.
  namespaces <- c("sparklyr", genv_get_extension_packages()) %>%
    purrr::map(asNamespace)

  (function(what, namespaces) {
    if (!length(namespaces)) {
      return(NULL)
    }

    # Look in `namespaces` one at a time for the function
    purrr::possibly(get, NULL)(
      what,
      envir = namespaces[[1]],
      mode = "function"
    ) %||%
      Recall(what, namespaces[-1])
  })(what, namespaces)
}

find_constructor <- function(candidates, jobj) {
  if (!length(candidates)) {
    stop(
      "Constructor not found for `",
      jobj_class(jobj)[[1]],
      "`.",
      call. = FALSE
    )
  }

  # For each candidate function, look in extension namespaces, and return the first one found
  find_in_extensions(candidates[[1]]) %||% Recall(candidates[-1])
}

ml_get_constructor <- function(jobj) {
  jobj %>%
    jobj_class(simple_name = FALSE) %>%
    purrr::map(ml_map_class) %>%
    purrr::compact() %>%
    purrr::map(~ paste0("new_", .x)) %>%
    find_constructor(jobj)
}

#' Wrap a Spark ML JVM object
#'
#' Identifies the associated sparklyr ML constructor for the JVM object by inspecting its
#'   class and performing a lookup. The lookup table is specified by the
#'   `sparkml/class_mapping.json` files of sparklyr and the loaded extensions.
#'
#' @param jobj The jobj for the pipeline stage.
#'
#' @keywords internal
#' @export
ml_call_constructor <- function(jobj) {
  do.call(ml_get_constructor(jobj), list(jobj = jobj))
}

new_ml_pipeline_stage <- function(jobj, ..., class = character()) {
  structure(
    list(
      uid = invoke(jobj, "uid"),
      param_map = ml_get_param_map(jobj),
      ...,
      .jobj = jobj
    ),
    class = c(class, "ml_pipeline_stage")
  )
}
