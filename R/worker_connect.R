spark_worker_connect <- function(sessionId, config) {
  gatewayPort <- spark_config_value(config, "sparklyr.worker.gateway.port", 8880)
  gatewayAddress <- spark_config_value(config, "sparklyr.worker.gateway.address", "localhost")
  config <- list()

  worker_log("is connecting to backend using port ", gatewayPort)

  gatewayInfo <- spark_connect_gateway(gatewayAddress,
                                       gatewayPort,
                                       sessionId,
                                       config = config,
                                       isStarting = TRUE)

  worker_log("is connected to backend")
  worker_log("is connecting to backend session")

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
    close(gatewayInfo$gateway)

    stop(
      "Failed to open connection to backend:", err$message
    )
  })

  worker_log("is connected to backend session")

  sc <- structure(class = c("spark_worker_connection"), list(
    # spark_connection
    master = "",
    method = "shell",
    app_name = NULL,
    config = NULL,
    # spark_shell_connection
    spark_home = NULL,
    backend = backend,
    monitor = gatewayInfo$gateway,
    output_file = NULL
  ))

  worker_log("created connection")

  sc
}

