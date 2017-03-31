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

  tryCatch({
    gatewayInfo <- spark_connect_gateway(gatewayAddress = "localhost",
                                        gatewayPort = gatewayPort,
                                        sessionId = gatewayPort,
                                        config = config,
                                        isStarting = TRUE)
  }, error = function(err) {
    stop(paste("Failed to open connection to sparklyr gateway:", err$message))
  })

  output_file <- tempfile(fileext = "_spark.log")
  error_file <- tempfile(fileext = "_spark.err")

  tryCatch({
    # set timeout for socket connection
    timeout <- spark_config_value(config, "sparklyr.backend.timeout", 30 * 24 * 60 * 60)
    backend <- socketConnection(host = "localhost",
                                port = gatewayInfo$backendPort,
                                server = FALSE,
                                blocking = TRUE,
                                open = "wb",
                                timeout = timeout)
  }, error = function(err) {
    stop(paste("Failed to open connection to sparklyr backend:", err$message))
  })

  # create the databricks connection
  sc <- structure(class = c("spark_connection", "spark_shell_connection"), list(
    # spark_connection
    master = "databricks",
    method = "shell",
    app_name = "Databricks",
    config = config,
    # spark_shell_connection
    spark_home = Sys.getenv("SPARK_HOME"),
    backend = backend,
    monitor = gatewayInfo$gateway,
    output_file = output_file
  ))

  # stop shell on R exit
  reg.finalizer(baseenv(), function(x) {
    if (connection_is_open(sc)) {
      stop_shell(sc)
    }
  }, onexit = TRUE)

  sc
}


