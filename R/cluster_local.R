#' Prepares the spark environment, downloading the spark binaries
#' to a common directory if needed.
#' @export
setup_local <- function(version = "1.6.0") {
  sparkInfo <- spark_install(version)

  sparkHome <- sparkInfo$sparkVersionDir
  Sys.setenv(SPARK_HOME = sparkHome)
}
