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

start_shell <- function() {
  sparkHome <- Sys.getenv("SPARK_HOME")
  if (nchar(sparkHome) == 0)
    stop("SPARK_HOME environment vairbale not set.")

  sparkSubmitByOs <- list(
    unix = "spark-submit",
    windows = "spark-submit.cmd"
  )

  sparkSubmit <- sparkSubmitByOs[[.Platform$OS.type]]
  sparkSubmitPath <- file.path(sparkHome, "bin", sparkSubmit)

  shellOutputPath <- tempfile(fileext = ".out")
  on.exit(unlink(shellOutputPath))

  sparkCommand <- paste("--packages com.databricks:spark-csv_2.11:1.2.0",
                        "sparkr-shell",
                        shellOutputPath)

  invisible(system2(sparkSubmitPath, sparkCommand, wait = F))

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

  con <- list(
    monitor = monitor,
    backend = backend,
    finalized = FALSE
  )

  reg.finalizer(baseenv(), function(x) {
    if (!con$finalized) {
      stop_shell(con)
    }
  }, onexit = TRUE)

  con
}

stop_shell <- function(con) {
  spark_api(con, FALSE, "0", "stop")
  con$finalized <- TRUE
}
