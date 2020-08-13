sparklyr_gateway_trouble_shooting_msg <- function() {
  c(
    "\n\n\nTry running `options(sparklyr.log.console = TRUE)` followed by ",
    "`sc <- spark_connect(...)` for more debugging info."
  )
}

wait_connect_gateway <- function(gatewayAddress, gatewayPort, config, isStarting) {
  waitSeconds <- if (isStarting) {
    spark_config_value(config, "sparklyr.connect.timeout", 60)
  } else {
    spark_config_value(config, "sparklyr.gateway.timeout", 1)
  }

  gateway <- NULL
  commandStart <- Sys.time()

  while (is.null(gateway) && Sys.time() < commandStart + waitSeconds) {
    tryCatch(
      {
        suppressWarnings({
          timeout <- spark_config_value(config, "sparklyr.gateway.interval", 1)
          gateway <- socketConnection(
            host = gatewayAddress,
            port = gatewayPort,
            server = FALSE,
            blocking = TRUE,
            open = "rb",
            timeout = timeout
          )
        })
      },
      error = function(err) {
      }
    )

    startWait <- spark_config_value(config, "sparklyr.gateway.wait", 50 / 1000)
    Sys.sleep(startWait)
  }

  gateway
}

spark_gateway_commands <- function() {
  list(
    "GetPorts" = 0,
    "RegisterInstance" = 1
  )
}

query_gateway_for_port <- function(gateway, sessionId, config, isStarting) {
  waitSeconds <- if (isStarting) {
    spark_config_value(config, "sparklyr.connect.timeout", 60)
  } else {
    spark_config_value(config, "sparklyr.gateway.timeout", 1)
  }

  writeInt(gateway, spark_gateway_commands()[["GetPorts"]])
  writeInt(gateway, sessionId)
  writeInt(gateway, if (isStarting) waitSeconds else 0)

  backendSessionId <- NULL
  redirectGatewayPort <- NULL

  commandStart <- Sys.time()
  while (length(backendSessionId) == 0 && commandStart + waitSeconds > Sys.time()) {
    backendSessionId <- readInt(gateway)
    Sys.sleep(0.1)
  }

  redirectGatewayPort <- readInt(gateway)
  backendPort <- readInt(gateway)

  if (length(backendSessionId) == 0 || length(redirectGatewayPort) == 0 || length(backendPort) == 0) {
    if (isStarting) {
      stop(
        "Sparklyr gateway did not respond while retrieving ports information after ",
        waitSeconds,
        " seconds.",
        sparklyr_gateway_trouble_shooting_msg()
      )
    } else {
      return(NULL)
    }
  }

  list(
    gateway = gateway,
    backendPort = backendPort,
    redirectGatewayPort = redirectGatewayPort
  )
}

spark_connect_gateway <- function(
                                  gatewayAddress,
                                  gatewayPort,
                                  sessionId,
                                  config,
                                  isStarting = FALSE) {

  # try connecting to existing gateway
  gateway <- wait_connect_gateway(gatewayAddress, gatewayPort, config, isStarting)

  if (is.null(gateway)) {
    if (isStarting) {
      stop(
        "Gateway in ", gatewayAddress, ":", gatewayPort, " did not respond.",
        sparklyr_gateway_trouble_shooting_msg()
      )
    }

    NULL
  }
  else {
    worker_log("is querying ports from backend using port ", gatewayPort)

    gateway_ports_query_attempts <- as.integer(
      spark_config_value(config, "sparklyr.gateway.port.query.attempts", 3L)
    )
    gateway_ports_query_retry_interval_s <- as.integer(
      spark_config_value(config, "sparklyr.gateway.port.query.retry.interval.seconds", 4L)
    )
    while (gateway_ports_query_attempts > 0) {
      gateway_ports_query_attempts <- gateway_ports_query_attempts - 1
      withCallingHandlers(
        {
          gatewayPortsQuery <- query_gateway_for_port(
            gateway,
            sessionId,
            config,
            isStarting
          )
          break
        },
        error = function(e) {
          isStarting <- FALSE
          if (gateway_ports_query_attempts > 0) {
            Sys.sleep(gateway_ports_query_retry_interval_s)
          }
          NULL
        }
      )
    }
    if (is.null(gatewayPortsQuery) && !isStarting) {
      close(gateway)
      return(NULL)
    }

    redirectGatewayPort <- gatewayPortsQuery$redirectGatewayPort
    backendPort <- gatewayPortsQuery$backendPort

    worker_log("found redirect gateway port ", redirectGatewayPort)

    if (redirectGatewayPort == 0) {
      close(gateway)

      if (isStarting) {
        stop("Gateway in ", gatewayAddress, ":", gatewayPort, " does not have the requested session registered")
      }

      NULL
    } else if (redirectGatewayPort != gatewayPort) {
      close(gateway)

      spark_connect_gateway(gatewayAddress, redirectGatewayPort, sessionId, config, isStarting)
    }
    else {
      list(
        gateway = gateway,
        backendPort = backendPort
      )
    }
  }
}
