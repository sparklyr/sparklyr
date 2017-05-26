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

create_hive_context_v2 <- function(sc) {

  # SparkSession.builder().enableHiveSupport()
  builder <- invoke_static(
    sc,
    "org.apache.spark.sql.SparkSession",
    "builder"
  )

  builder <- invoke(
    builder,
    "enableHiveSupport"
  )

  session <- invoke(
    builder,
    "getOrCreate"
  )

  # get config object
  conf <- invoke(session, "conf")

  # apply spark.sql. params
  params <- connection_config(sc, "spark.sql.")
  apply_config(params, conf, "set", "spark.sql.")

  # return session as hive context
  session
}
