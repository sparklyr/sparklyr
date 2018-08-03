#' A helper function to retrieve values from \code{spark_config()}
#'
#' @param config The configuration list from \code{spark_config()}
#' @param name The name of the configuration entry
#' @param default The default value to use when entry is not present
#'
#' @keywords internal
#' @export
spark_config_value <- function(config, name, default = NULL) {
  if (!name %in% names(config))
    default
  else {
    value <- config[[name]]
    if (is.function(value)) value <- value()
    value
  }
}

spark_config_value_retries <- function(config, name, default, retries) {
  success <- FALSE
  value <- default

  while (!success && retries > 0) {
    retries <- retries - 1

    tryCatch({
      value <<- spark_config_value(config, name, default)
      success <<- TRUE
    }, error = function(e) {
      if (sparklyr_boolean_option("sparklyr.verbose")) {
        message("Reading ", name, " failed with error: ", e$message)
      }

      if (retries > 0) Sys.sleep(1)
    })
  }

  if (!success) {
    stop("Failed after ", retries, " attempts while reading conf value ", name)
  }

  value
}

spark_config_integer <- function(config, name, default = NULL) {
  as.integer(spark_config_value(config, name, default))
}

spark_config_logical <- function(config, name, default = NULL) {
  as.logical(spark_config_value(config, name, default))
}
