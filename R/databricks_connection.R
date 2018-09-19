databricks_connection <- function(config, extensions) {
  tryCatch({
    callSparkR <- get("callJStatic", envir = asNamespace("SparkR"))
    gatewayPort <- as.numeric(callSparkR("com.databricks.backend.daemon.driver.RDriverLocal",
                                         "startSparklyr",
                                         # In Databricks notebooks, this variable is in the default namespace
                                         get("DATABRICKS_GUID", envir = .GlobalEnv),
                                         # startSparklyr will search & find proper JAR file
                                         system.file("java/", package = "sparklyr")))
  }, error = function(err) {
    stop(paste("Failed to start sparklyr backend:", err$message))
  })

  new_databricks_connection(
    gateway_connection(
      paste("sparklyr://localhost:", gatewayPort, "/", gatewayPort, sep = ""),
      config = config
    )
  )
}

#' @export
spark_version.databricks_connection <- function(sc) {
  if (!is.null(sc$state$spark_version))
    return(sc$state$spark_version)

  version <- eval(rlang::parse_expr("SparkR::sparkR.version()"))

  sc$state$spark_version <- numeric_version(version)

  # return to caller
  sc$state$spark_version
}

new_databricks_connection <- function(scon) {
  new_spark_gateway_connection(
    scon,
    subclass = "databricks_connection"
  )
}
