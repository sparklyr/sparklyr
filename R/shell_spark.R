read_shell_file <- function(shellFile) {
  shellOutputFile <- file(shellFile, open = "rb")
  backendPort <- readInt(shellOutputFile)
  monitorPort <- readInt(shellOutputFile)
  rLibraryPath <- readString(shellOutputFile)
  close(shellOutputFile)

  success <- length(backendPort) > 0 && backendPort > 0 &&
    length(monitorPort) > 0 && monitorPort > 0 &&
    length(rLibraryPath) == 1

  if (!success)
    stop("Invalid values found in shell output")

  list(
    backendPort = backendPort,
    monitorPort = monitorPort
  )
}

start_shell <- function(sconInst, installInfo, packages) {
  sparkHome <- installInfo$sparkVersionDir
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
  if (length(packages) > 0) {
    sparkCommand <- paste("--packages ", paste(packages, sep = ","))
  }

  sparkCommand <- paste(sparkCommand, "sparkr-shell", shellOutputPath)

  outputFile <- tempfile(fileext = "_spark.log")
  invisible(system2(sparkSubmitPath, sparkCommand, wait = F, stdout = outputFile, stderr = outputFile))

  if (!wait_file_exists(shellOutputPath))
    stop("Failed to launch Spark shell. Ports file does not exist")

  shellFile <- read_shell_file(shellOutputPath)

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
  spark_invoke_method(scon, FALSE, "0", "stop")

  sconInst <- spark_connection_remove_inst(scon)

  close(sconInst$backend)
  close(sconInst$monitor)

  on_connection_closed(scon)
}

