spark_worker_main <- function(sessionId) {
  log_file <- file.path("~", "spark", basename(tempfile(fileext = ".log")))

  log <- function(message) {
    write(paste0(message, "\n"), file = log_file, append = TRUE)
    cat("sparkworker:", message, "\n")
  }

  log("sparklyr worker starting")
  log("sparklyr connecting to backend")

  gatewayPort <- "8880"
  gatewayAddress <- "localhost"

  gatewayInfo <- spark_connect_gateway(gatewayAddress,
                                       gatewayPort,
                                       sessionId,
                                       config = config,
                                       isStarting = TRUE)

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

    abort_shell(
      paste("Failed to open connection to backend:", err$message),
      spark_submit_path,
      shell_args,
      output_file,
      error_file
    )
  })

  log("sparklyr worker finished")
}
