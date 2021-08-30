databricks_expand_jars <- function() {
  jars_path <- system.file("java/", package = "sparklyr")

  missing_versions <- c(
    "2.2",
    "2.1"
  )

  for (missing in missing_versions) {
    compatible_jar <- spark_default_app_jar(paste0(missing, ".0"))
    target_jar <- paste0("sparklyr-", missing, "-2.11.jar")

    if (!file.exists(file.path(jars_path, target_jar))) {
      file.copy(
        compatible_jar,
        file.path(jars_path, target_jar)
      )
    }
  }
}

databricks_connection <- function(config, extensions) {
  tryCatch(
    {
      databricks_expand_jars()

      callSparkR <- get("callJStatic", envir = asNamespace("SparkR"))
      # In Databricks environments (notebooks & rstudio) DATABRICKS_GUID is in the default namespace
      guid <- get("DATABRICKS_GUID", envir = .GlobalEnv)
      gatewayPort <- as.numeric(callSparkR(
        "com.databricks.backend.daemon.driver.RDriverLocal",
        "startSparklyr",
        guid,
        # startSparklyr will search & find proper JAR file
        system.file("java/", package = "sparklyr")
      ))
    },
    error = function(err) {
      stop("Failed to start sparklyr backend: ", err$message)
    }
  )

  # Hand in driver's libPaths to worker if user hasn't already set libpaths for notebook-scoped libraries
  config$spark.r.libpaths <- config$spark.r.libpaths %||% paste(.libPaths(), collapse = ",")

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
  if (!is.null(sc$state$spark_version)) {
    return(sc$state$spark_version)
  }

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
  # In databricks, sparklyr should use the SqlContext associated with the RDriverLocal instance for
  # this guid.
  r_driver_local <- "com.databricks.backend.daemon.driver.RDriverLocal"
  session <- invoke_static(sc, r_driver_local, "getDriver", guid) %>%
    invoke("sqlContext") %>%
    invoke("sparkSession")
  # This is called hive_context but for spark > 2.0, it should actually be a spark session
  sc$state$hive_context <- session
  sc
}
