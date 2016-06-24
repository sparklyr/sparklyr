spark_install_windows_local <- function() {
  hivePath <- "\\tmp\\hive"
  if (!dir.exists(hivePath)) {
    dir.create(hivePath, recursive = TRUE)
    message(paste("Created hive default directory under:", hivePath))
  }
}