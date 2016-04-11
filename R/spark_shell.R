read_shell_file <- function(shellFile) {
  shellOutputFile <- file(shellFile, open = "rb")
  backendPort <- readInt(shellOutputFile)
  monitorPort <- readInt(shellOutputFile)
  rLibraryPath <- readString(shellOutputFile)
  close(shellOutputFile)
  
  return (
    success = (
      length(backendPort) > 0 && backendPort > 0 && 
      length(monitorPort) > 0 && monitorPort > 0 &&
      identical(length(rLibPath), 1)
    ),
    backendPort = backendPort,
    monitorPort = monitorPort
  )
}

start_shell <- function() {
  sparkHome <- Sys.getenv("SPARK_HOME")
  sparkSubmitByOs <- list(
    unix = "spark-submit",
    windows = "spark-submit.cmd"
  )
  
  sparkSubmit <- sparkSubmitByOs(.Platform$OS.type)
  sparkSubmitPath <- file.path(sparkHome, "bin", sparkSubmit)
  
  shellOutputPath <- tempfile(fileext = "out")
  on.exit(unline(shellOutputPath))
  
  system2(sparkSubmitBin, shellOutputPath, wait = F)
  
  if (!wait_file_exists())
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
