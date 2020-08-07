#' A helper function to retrieve values from \code{spark_config()}
#'
#' @param config The configuration list from \code{spark_config()}
#' @param name The name of the configuration entry
#' @param default The default value to use when entry is not present
#'
#' @keywords internal
#' @export
spark_config_value <- function(config, name, default = NULL) {
  if (getOption("sparklyr.test.enforce.config", FALSE) && any(grepl("^sparklyr.", name))) {
    settings <- get("spark_config_settings")()
    if (!any(name %in% settings$name) &&
      !grepl("^sparklyr\\.shell\\.", name)) {
      stop("Config value '", name[[1]], "' not described in spark_config_settings()")
    }
  }

  name_exists <- name %in% names(config)
  if (!any(name_exists)) {
    name_exists <- name %in% names(options())
    if (!any(name_exists)) {
      value <- default
    } else {
      name_primary <- name[name_exists][[1]]
      value <- getOption(name_primary)
    }
  } else {
    name_primary <- name[name_exists][[1]]
    value <- config[[name_primary]]
  }

  if (is.language(value)) value <- rlang::as_closure(value)
  if (is.function(value)) value <- value()
  value
}

spark_config_integer <- function(config, name, default = NULL) {
  as.integer(spark_config_value(config, name, default))
}

spark_config_logical <- function(config, name, default = NULL) {
  as.logical(spark_config_value(config, name, default))
}
