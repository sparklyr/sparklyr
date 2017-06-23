worker_config_serialize <- function(config) {
  paste(
    if (isTRUE(config$debug)) "TRUE" else "FALSE",
    collapse = ";"
  )
}

worker_config_deserialize <- function(raw) {
  parts <- strsplit(raw, ";")[[1]]

  list(
    debug = as.logical(parts[[1]])
  )
}
