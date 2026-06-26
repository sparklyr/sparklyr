sparklyr_gateway_trouble_shooting_msg <- function() {
  c(
    "\n\n\nTry running `options(sparklyr.log.console = TRUE)` followed by ",
    "`sc <- spark_connect(...)` for more debugging info."
  )
}

wait_connect_gateway <- function(
  gatewayAddress,
  gatewayPort,
  config,
  isStarting
) {
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
      error = function(err) {}
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
  while (
    length(backendSessionId) == 0 && commandStart + waitSeconds > Sys.time()
  ) {
    backendSessionId <- readInt(gateway)
    Sys.sleep(0.1)
  }

  redirectGatewayPort <- readInt(gateway)
  backendPort <- readInt(gateway)

  if (
    length(backendSessionId) == 0 ||
      length(redirectGatewayPort) == 0 ||
      length(backendPort) == 0
  ) {
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
  isStarting = FALSE
) {
  # try connecting to existing gateway
  gateway <- wait_connect_gateway(
    gatewayAddress,
    gatewayPort,
    config,
    isStarting
  )

  if (is.null(gateway)) {
    if (isStarting) {
      stop(
        "Gateway in ",
        gatewayAddress,
        ":",
        gatewayPort,
        " did not respond.",
        sparklyr_gateway_trouble_shooting_msg()
      )
    }

    NULL
  } else {
    worker_log("is querying ports from backend using port ", gatewayPort)

    gateway_ports_query_attempts <- as.integer(
      spark_config_value(config, "sparklyr.gateway.port.query.attempts", 3L)
    )
    gateway_ports_query_retry_interval_s <- as.integer(
      spark_config_value(
        config,
        "sparklyr.gateway.port.query.retry.interval.seconds",
        4L
      )
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
        stop(
          "Gateway in ",
          gatewayAddress,
          ":",
          gatewayPort,
          " does not have the requested session registered"
        )
      }

      NULL
    } else if (redirectGatewayPort != gatewayPort) {
      close(gateway)

      spark_connect_gateway(
        gatewayAddress,
        redirectGatewayPort,
        sessionId,
        config,
        isStarting
      )
    } else {
      list(
        gateway = gateway,
        backendPort = backendPort
      )
    }
  }
}

#' @include connection_shell.R

master_is_gateway <- function(master) {
  length(grep("^(sparklyr://)?[^:]+:[0-9]+(/[0-9]+)?$", master)) > 0
}

gateway_connection <- function(master, config) {
  if (!master_is_gateway(master)) {
    stop(
      "sparklyr gateway master expected to be formatted as sparklyr://address:port"
    )
  }

  protocol <- strsplit(master, "//")[[1]]
  components <- strsplit(protocol[[2]], ":")[[1]]
  gatewayAddress <- components[[1]]
  portAndSesssion <- strsplit(components[[2]], "/")[[1]]
  gatewayPort <- as.integer(portAndSesssion[[1]])
  sessionId <- if (length(portAndSesssion) > 1) {
    as.integer(portAndSesssion[[2]])
  } else {
    0
  }

  gatewayInfo <- spark_connect_gateway(
    gatewayAddress = gatewayAddress,
    gatewayPort = gatewayPort,
    sessionId = sessionId,
    config = config
  )

  if (is.null(gatewayInfo)) {
    stop("Failed to connect to gateway: ", master)
  }

  sc <- spark_gateway_connection(master, config, gatewayInfo, gatewayAddress)

  if (is.null(gatewayInfo)) {
    stop("Failed to open connection from gateway: ", master)
  }

  sc
}

spark_gateway_connection <- function(
  master,
  config,
  gatewayInfo,
  gatewayAddress
) {
  tryCatch(
    {
      interval <- spark_config_value(config, "sparklyr.backend.interval", 1)

      backend <- socketConnection(
        host = gatewayAddress,
        port = gatewayInfo$backendPort,
        server = FALSE,
        blocking = interval > 0,
        open = "wb",
        timeout = interval
      )
      class(backend) <- c(class(backend), "shell_backend")

      monitoring <- socketConnection(
        host = gatewayAddress,
        port = gatewayInfo$backendPort,
        server = FALSE,
        blocking = interval > 0,
        open = "wb",
        timeout = interval
      )
      class(monitoring) <- c(class(monitoring), "shell_backend")
    },
    error = function(err) {
      close(gatewayInfo$gateway)
      stop("Failed to open connection to backend:", err$message)
    }
  )

  # create the shell connection
  sc <- new_spark_gateway_connection(list(
    # spark_connection
    master = master,
    method = "gateway",
    app_name = "sparklyr",
    config = config,
    state = new.env(),
    # spark_gateway_connection : spark_shell_connection
    spark_home = NULL,
    backend = backend,
    monitoring = monitoring,
    gateway = gatewayInfo$gateway,
    output_file = NULL
  ))

  # stop shell on R exit
  reg.finalizer(
    baseenv(),
    function(x) {
      if (connection_is_open(sc)) {
        stop_shell(sc)
      }
    },
    onexit = TRUE
  )

  sc
}

#' @export
connection_is_open.spark_gateway_connection <- connection_is_open.spark_shell_connection

#' @export
spark_log.spark_gateway_connection <- function(
  sc,
  n = 100,
  filter = NULL,
  ...
) {
  stop(
    "spark_log is not available while connecting through an sparklyr gateway"
  )
}

#' @export
spark_web.spark_gateway_connection <- function(sc, ...) {
  stop(
    "spark_web is not available while connecting through an sparklyr gateway"
  )
}

#' @export
invoke_method.spark_gateway_connection <- invoke_method.spark_shell_connection

#' @export
j_invoke_method.spark_gateway_connection <- j_invoke_method.spark_shell_connection

#' @export
print_jobj.spark_gateway_connection <- print_jobj.spark_shell_connection
