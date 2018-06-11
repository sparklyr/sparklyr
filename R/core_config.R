#' A helper function to retrieve values from \code{spark_config()}
#'
#' @param config The configuration list from \code{spark_config()}
#' @param name The name of the configuration entry
#' @param default The default value to use when entry is not present
#'
#' @keywords internal
#' @export
spark_config_value <- function(config, name, default = NULL) {
  matches <- name %in% names(config)
  if (!any(matches))
    default
  else
    config[[name]][[1]]
}
