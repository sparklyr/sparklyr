databricks_connection <- function(config, extensions) {
  tryCatch({
    callSparkR <- get("callJStatic", envir = asNamespace("SparkR"))
    # In Databricks notebooks, this variable is in the default namespace
    guid <- get("DATABRICKS_GUID", envir = .GlobalEnv)
    gatewayPort <- as.numeric(callSparkR("com.databricks.backend.daemon.driver.RDriverLocal",
                                         "startSparklyr",
                                         guid,
                                         # startSparklyr will search & find proper JAR file
                                         system.file("java/", package = "sparklyr")))
  }, error = function(err) {
    stop("Failed to start sparklyr backend: ", err$message)
  })

  new_databricks_connection(
    gateway_connection(
      paste("sparklyr://localhost:", gatewayPort, "/", gatewayPort, sep = ""),
      config = config
    ),
    guid
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

new_databricks_connection <- function(scon, guid) {
  sc <- new_spark_gateway_connection(
    scon,
    class = "databricks_connection"
  )
  rDriverLocal <- "com.databricks.backend.daemon.driver.RDriverLocal"
  hive_context <- invoke_static(sc, rDriverLocal, "getDriver", guid) %>% invoke("sqlContext")
  sc$state$hive_context <- hive_context
  sc
}
