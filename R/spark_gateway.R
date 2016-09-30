wait_connect_gateway <- function(gatewayAddress, gatewayPort, seconds) {
  retries <- seconds * 10
  gateway <- NULL

  while(is.null(gateway) && retries >= 0) {
    tryCatch({
      suppressWarnings(
        gateway <- socketConnection(host = gatewayAddress,
                                    port = gatewayPort,
                                    server = FALSE,
                                    blocking = TRUE,
                                    open = "rb",
                                    timeout = 6000)
      )
    }, error = function(err) {
    })

    retries <- retries  - 1;
    Sys.sleep(0.1)
  }

  gateway
}

spark_gateway_commands <- function() {
  list(
    "GetPorts" = 0,
    "RegisterInstance" = 1
  )
}

spark_connect_gateway <- function(gatewayAddress, gatewayPort, sessionId, waitSeconds) {
  # try connecting to existing gateway
  gateway <- wait_connect_gateway(gatewayAddress, gatewayPort, waitSeconds)

  if (is.null(gateway)) {
    NULL
  }
  else {
    writeInt(gateway, spark_gateway_commands()[["GetPorts"]])
    writeInt(gateway, sessionId)

    backendSessionId <- readInt(gateway)
    redirectGatewayPort <- readInt(gateway)
    backendPort <- readInt(gateway)

    if (length(backendSessionId) == 0 || length(redirectGatewayPort) == 0 || length(backendPort) == 0) {
      stop("Sparklyr gateway did not respond while retrieving ports information")
    }

    if (redirectGatewayPort != gatewayPort) {
      close(gateway)
      gateway <- wait_connect_gateway(gatewayAddress, redirectGatewayPort, 1)
    }

    list(
      gateway = gateway,
      backendPort = backendPort
    )
  }
}

gateway_connection <- function(master, config) {
  gatewayPort <- as.integer(spark_config_value(config, "sparklyr.gateway.port", "8880"))
  gatewayAddress <- spark_config_value(config, "sparklyr.gateway.address", master)
  sessionId <- 0

  timeout <- spark_config_value(config, "sparklyr.gateway.remote.timeout", 3)
  gatewayInfo <- spark_connect_gateway(address = gatewayAddress,
                                       port = gatewayPort,
                                       sessionId = sessionId,
                                       waitSeconds = timeout)

  if (is.null(gatewayInfo)) {
    stop("Failed to connect to gateway: ", master)
  }

  sc <- spark_gateway_connection(config, gatewayInfo)

  if (is.null(gatewayInfo)) {
    stop("Failed to open connection from gateway: ", master)
  }

  sc
}

spark_gateway_connection <- function(config, gatewayInfo) {
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
    stop("Failed to open connection to backend:", err$message)
  })

  # create the shell connection
  sc <- structure(class = c("spark_connection", "spark_gateway_connection"), list(
    # spark_connection
    master = master,
    method = "gateway",
    app_name = "?",
    config = config,
    # spark_gateway_connection
    gatewayInfo = gatewayInfo
  ))

  # stop shell on R exit
  reg.finalizer(baseenv(), function(x) {
    if (connection_is_open(sc)) {
      stop_shell(sc, FALSE)
    }
  }, onexit = TRUE)

  # initialize and return the connection
  tryCatch({
    sc <- initialize_connection(sc)
  }, error = function(e) {
    stop("Failed during initialize_connection:", e$message)
  })

  sc
}
