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
  object <- if (spark_version(sc) < "2.0.0") {
    "sparklyr.MLUtils"
  } else {
    "sparklyr.MLUtils2"
  }

  invoke_static(
    sc,
    object,
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
        function(nm) rlang::env_get(mapping, nm, default = NULL, inherit = TRUE) %||% nm
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
      function(nm) rlang::env_get(mapping, nm, default = NULL, inherit = TRUE) %||% nm
    )
  )
}
