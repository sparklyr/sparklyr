#' Prepares the spark environment, downloading the spark binaries
#' to a common directory if needed.
#' @export
setup_local <- function(version = "1.6.0") {
  sparkInfo <- download_spark(version)

  sparkHome <- sparkInfo$sparkVersionDir
  Sys.setenv(SPARK_HOME = sparkHome)
  .libPaths(c(file.path(sparkHome, "R", "lib")))
}
