# nocov start

spark_worker_connect <- function(
  sessionId,
  backendPort = 8880,
  config = list()) {

  gatewayPort <- spark_config_value(config, "sparklyr.worker.gateway.port", backendPort)

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
    interval <- spark_config_value(config, "sparklyr.backend.interval", 1)

    backend <- socketConnection(host = "localhost",
                                port = gatewayInfo$backendPort,
                                server = FALSE,
                                blocking = interval > 0,
                                open = "wb",
                                timeout = interval)

    class(backend) <- c(class(backend), "shell_backend")
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
    state = new.env(),
    # spark_shell_connection
    spark_home = NULL,
    backend = backend,
    gateway = gatewayInfo$gateway,
    output_file = NULL
  ))
  sc$state$validJobjs <- new.env(parent = emptyenv())
  sc$state$toRemoveJobjs <- new.env(parent = emptyenv())

  worker_log("created connection")

  sc
}

# nocov end
