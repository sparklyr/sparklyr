#' @include shell_connection.R

master_is_gateway <- function(master) {
  length(grep("^(sparklyr://)?[^:]+:[0-9]+(/[0-9]+)?$", master)) > 0
}

gateway_connection <- function(master, config) {
  if (!master_is_gateway(master)) {
    stop("sparklyr gateway master expected to be formatted as sparklyr://address:port")
  }

  protocol <- strsplit(master, "//")[[1]]
  components <- strsplit(protocol[[2]], ":")[[1]]
  gatewayAddress <- components[[1]]
  portAndSesssion <- strsplit(components[[2]], "/")[[1]]
  gatewayPort <- as.integer(portAndSesssion[[1]])
  sessionId <- if (length(portAndSesssion) > 1) as.integer(portAndSesssion[[2]]) else 0

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

spark_gateway_connection <- function(master, config, gatewayInfo, gatewayAddress) {
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
  reg.finalizer(baseenv(), function(x) {
    if (connection_is_open(sc)) {
      stop_shell(sc)
    }
  }, onexit = TRUE)

  sc
}

#' @export
connection_is_open.spark_gateway_connection <- connection_is_open.spark_shell_connection

#' @export
spark_log.spark_gateway_connection <- function(sc, n = 100, filter = NULL, ...) {
  stop("spark_log is not available while connecting through an sparklyr gateway")
}

#' @export
spark_web.spark_gateway_connection <- function(sc, ...) {
  stop("spark_web is not available while connecting through an sparklyr gateway")
}

#' @export
invoke_method.spark_gateway_connection <- invoke_method.spark_shell_connection

#' @export
print_jobj.spark_gateway_connection <- print_jobj.spark_shell_connection
