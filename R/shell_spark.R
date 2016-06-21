
start_shell <- function(scon, sconInst, jars, packages) {
  sparkHome <- scon$installInfo$sparkVersionDir
  if (!dir.exists(sparkHome)) {
    stop("Spark installation was not found. See spark_install.")
  }

  sparkSubmitByOs <- list(
    unix = "spark-submit",
    windows = "spark-submit.cmd"
  )

  sparkSubmit <- sparkSubmitByOs[[.Platform$OS.type]]
  sparkSubmitPath <- file.path(sparkHome, "bin", sparkSubmit)

  shellOutputPath <- tempfile(fileext = ".out")
  on.exit(unlink(shellOutputPath))

  sparkCommand <- ""

  parameters <- scon$config[["sparklyr.shell."]]
  parameters <- if(is.null(parameters)) list() else parameters

  parameters[["packages"]] <- unique(c(parameters[["--packages"]], packages))
  parameters[["jars"]] <- unique(c(parameters[["--jars"]], jars))

  lapply(names(parameters), function(paramName) {
    paramValue <- parameters[[paramName]]
    if (!is.null(paramValue)) {
      sparkCommand <<- paste0(sparkCommand, 
                              shQuote(paste0("--", paramName)), " ", 
                              shQuote(paste(paramValue, collapse = ",")), " ")
    }
  })

  sparkCommand <- paste(sparkCommand, "sparkr-shell", shellOutputPath)

  outputFile <- tempfile(fileext = "_spark.log")

  env <- character()
  if (spark_connection_is_local(scon))
    env <- paste0("SPARK_LOCAL_IP=127.0.0.1")

  invisible(system2(sparkSubmitPath, sparkCommand, wait = FALSE, env = env, stdout = outputFile, stderr = outputFile))

  if (!wait_file_exists(shellOutputPath)) {
    stop(paste(
      "Failed to launch Spark shell. Ports file does not exist.\n",
      "    Path: ", sparkSubmitPath, "\n",
      "    Parameters: ", sparkCommand,
      sep = ""))
  }

  shellFile <- sparkapi_read_shell_file(shellOutputPath)

  tryCatch({
    monitor <- socketConnection(port = shellFile$monitorPort)
  }, error = function(err) {
    stop("Failed to open connection to monitor")
  })

  tryCatch({
    backend <- socketConnection(host = "localhost",
                                port = shellFile$backendPort,
                                server = FALSE,
                                blocking = TRUE,
                                open = "wb",
                                timeout = 6000)
  }, error = function(err) {
    stop("Failed to open connection to backend")
  })

  sconInst$monitor <- monitor
  sconInst$backend <- backend
  sconInst$outputFile <- outputFile

  sconInst
}

stop_shell <- function(scon) {
  
  sparkapi_stop_backend(sparkapi_connection(scon))

  sconInst <- spark_connection_remove_inst(scon)

  close(sconInst$backend)
  close(sconInst$monitor)

  on_connection_closed(scon)
}

