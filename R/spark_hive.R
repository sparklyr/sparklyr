create_hive_context <- function(sc) {
  UseMethod("create_hive_context")
}

apply_config <- function(params, object, method, prefix) {
  lapply(names(params), function(paramName) {
    configValue <- params[[paramName]]
    if (is.logical(configValue)) {
      configValue <- if (configValue) "true" else "false"
    }
    else {
      configValue <- as.character(configValue)
    }

    invoke(
      object,
      method,
      paste0(prefix, paramName),
      configValue
    )
  })
}
