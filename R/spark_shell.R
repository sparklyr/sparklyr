read_shell_file <- function(shellFile) {
  shellOutputFile <- file(shellFile, open = "rb")
  backendPort <- readInt(shellOutputFile)
  monitorPort <- readInt(shellOutputFile)
  rLibraryPath <- readString(shellOutputFile)
  close(shellOutputFile)

  success <- length(backendPort) > 0 && backendPort > 0 &&
    length(monitorPort) > 0 && monitorPort > 0 &&
    identical(length(rLibraryPath), 1)

  list(
    success = success,
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

  sparkCommand <- paste("sparkr-shell", shellOutputPath)
  system2(sparkSubmitPath, sparkCommand, wait = F)

  if (!wait_file_exists(shellOutputPath))
    stop("Failed to launch Spark shell. Ports file does not exist")

  shellFile <- read_shell_file(shellOutputPath)

  tryCatch({
    monitor <- socketConnection(port = shellFile$monitorPort)
  }, error = function(err) {
    stop("Failed to open connection to monitor")
  })

  tryCatch({
    backend <- socketConnection(port = shellFile$backendPort)
  }, error = function(err) {
    stop("Failed to open connection to backend")
  })

  return(list(
    monitor = monitor,
    backend = backend
  ))
}

stop_shell <- function(con) {
  spark_api(con$backend, FALSE, "0", "stop")
}
