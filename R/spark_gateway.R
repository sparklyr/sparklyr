wait_connect_gateway <- function(gatewayAddress, gatewayPort, config, isStarting) {
  retries <- if (isStarting)
    spark_config_value(config, "sparklyr.gateway.start.timeout", 60)
  else
    spark_config_value(config, "sparklyr.gateway.connect.timeout", 1)

  gateway <- NULL
  commandStart <- NULL

  while (is.null(gateway) && retries >= 0) {
    commandStart <- Sys.time()

    tryCatch({
      suppressWarnings({
        timeout <- spark_config_value(config, "sparklyr.monitor.timeout", 1)
        gateway <- socketConnection(host = gatewayAddress,
                                    port = gatewayPort,
                                    server = FALSE,
                                    blocking = TRUE,
                                    open = "rb",
                                    timeout = timeout)
      })
    }, error = function(err) {
    })

    retries <- retries  - 1;

    # wait for one second without depending on the behavior of socketConnection timeout
    while (commandStart + 1 > Sys.time()) {
      Sys.sleep(0.1)
    }
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
  waitSeconds <- if (isStarting)
    spark_config_value(config, "sparklyr.gateway.start.timeout", 60)
  else
    spark_config_value(config, "sparklyr.gateway.connect.timeout", 1)

  writeInt(gateway, spark_gateway_commands()[["GetPorts"]])
  writeInt(gateway, sessionId)
  writeInt(gateway, if (isStarting) waitSeconds else 0)

  backendSessionId <- NULL
  redirectGatewayPort <- NULL

  commandStart <- Sys.time()
  while(length(backendSessionId) == 0 && commandStart + waitSeconds > Sys.time()) {
    backendSessionId <- readInt(gateway)
    Sys.sleep(0.1)
  }

  redirectGatewayPort <- readInt(gateway)
  backendPort <- readInt(gateway)

  if (length(backendSessionId) == 0 || length(redirectGatewayPort) == 0 || length(backendPort) == 0) {
    stop("Sparklyr gateway did not respond while retrieving ports information after ", waitSeconds, " seconds")
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
    if (isStarting)
      stop(
        "Gateway in port (", gatewayPort, ") did not respond.")

    NULL
  }
  else {
    gatewayPortsQuery <- query_gateway_for_port(gateway, sessionId, config, isStarting)
    redirectGatewayPort <- gatewayPortsQuery$redirectGatewayPort
    backendPort <- gatewayPortsQuery$backendPort

    if (redirectGatewayPort == 0) {
      close(gateway)

      if (isStarting)
        stop("Gateway in port (", gatewayPort, ") does not have the requested session registered")

      NULL
    } else if(redirectGatewayPort != gatewayPort) {
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

master_is_gateway <- function(master) {
  length(grep("^(sparklyr://)?[^:]+:[0-9]+$", master)) > 0
}

gateway_connection <- function(master, config) {
  if (!master_is_gateway(master)) {
    stop("sparklyr gateway master expected to be formatted as sparklyr://address:port")
  }

  protocol <- strsplit(master, "//")[[1]]
  components <- strsplit(protocol[[2]], ":")[[1]]
  gatewayAddress <- components[[1]]
  gatewayPort <- as.integer(components[[2]])
  sessionId <- 0

  gatewayInfo <- spark_connect_gateway(gatewayAddress = gatewayAddress,
                                       gatewayPort = gatewayPort,
                                       sessionId = sessionId,
                                       config = config)

  if (is.null(gatewayInfo)) {
    stop("Failed to connect to gateway: ", master)
  }

  sc <- spark_gateway_connection(master, config, gatewayInfo, gatewayAddress)

  if (is.null(gatewayInfo)) {
    stop("Failed to open connection from gateway: ", master)
  }

  sc
}

spark_gateway_connection <- function(master, config, gatewayInfo, gatewayAddress) {
  tryCatch({
    # set timeout for socket connection
    timeout <- spark_config_value(config, "sparklyr.backend.timeout", 60)
    backend <- socketConnection(host = gatewayAddress,
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
    app_name = "",
    config = config,
    # spark_gateway_connection : spark_shell_connection
    spark_home = NULL,
    backend = backend,
    monitor = gatewayInfo$gateway,
    output_file = NULL
  ))

  # stop shell on R exit
  reg.finalizer(baseenv(), function(x) {
    if (connection_is_open(sc)) {
      stop_shell(sc)
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

#' @export
connection_is_open.spark_gateway_connection <- function(sc) {
  class(sc) <- "spark_shell_connection"
  connection_is_open(sc)
}

#' @export
spark_log.spark_gateway_connection <- function(sc, n = 100, filter = NULL, ...) {
  stop("spark_log is not available while connecting thorugh an sparklyr gateway")
}

#' @export
spark_web.spark_gateway_connection <- function(sc, ...) {
  stop("spark_web is not available while connecting thorugh an sparklyr gateway")
}

#' @export
invoke_method.spark_gateway_connection <- function(sc, static, object, method, ...) {
  class(sc) <- "spark_shell_connection"
  invoke_method(sc, static, object, method, ...)
}

#' @export
print_jobj.spark_gateway_connection <- function(sc, jobj, ...) {
  class(sc) <- "spark_shell_connection"
  print_jobj(sc, jobj, ...)
}
