worker_config_serialize <- function(config) {
  paste(
    if (isTRUE(config$debug)) "TRUE" else "FALSE",
    spark_config_value(config, "sparklyr.worker.gateway.port", "8880"),
    spark_config_value(config, "sparklyr.worker.gateway.address", "localhost"),
    if (isTRUE(config$profile)) "TRUE" else "FALSE",
    if (isTRUE(config$schema)) "TRUE" else "FALSE",
    if (isTRUE(config$arrow)) "TRUE" else "FALSE",
    if (isTRUE(config$fetch_result_as_sdf)) "TRUE" else "FALSE",
    if (isTRUE(config$single_binary_column)) "TRUE" else "FALSE",
    config$spark_version,
    sep = ";"
  )
}

worker_config_deserialize <- function(raw) {
  parts <- strsplit(raw, ";")[[1]]

  list(
    debug = as.logical(parts[[1]]),
    sparklyr.gateway.port = as.integer(parts[[2]]),
    sparklyr.gateway.address = parts[[3]],
    profile = as.logical(parts[[4]]),
    schema = as.logical(parts[[5]]),
    arrow = as.logical(parts[[6]]),
    fetch_result_as_sdf = as.logical(parts[[7]]),
    single_binary_column = as.logical(parts[[8]]),
    spark_version = parts[[9]]
  )
}
