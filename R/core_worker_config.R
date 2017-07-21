worker_config_serialize <- function(config) {
  paste(
    if (isTRUE(config$debug)) "TRUE" else "FALSE",
    spark_config_value(config, "sparklyr.worker.gateway.port", "8880"),
    spark_config_value(config, "sparklyr.worker.gateway.address", "localhost"),
    sep = ";"
  )
}

worker_config_deserialize <- function(raw) {
  parts <- strsplit(raw, ";")[[1]]

  list(
    debug = as.logical(parts[[1]]),
    sparklyr.gateway.port = as.integer(parts[[2]]),
    sparklyr.gateway.address = parts[[3]]
  )
}
