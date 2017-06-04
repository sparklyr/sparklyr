
spark_config_value <- function(config, name, default = NULL) {
  if (!name %in% names(config)) default else config[[name]]
}
