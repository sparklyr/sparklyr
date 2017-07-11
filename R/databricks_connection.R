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

  gateway_connection(
    paste("sparklyr://localhost:", gatewayPort, "/", gatewayPort, sep = ""),
    config = config)
}


