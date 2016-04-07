setup_local <- function(version) {
  sparkInfo <- download_spark(version)

  sparkHome <- sparkInfo$sparkVersionDir
  Sys.setenv(SPARK_HOME = sparkHome)
  .libPaths(c(file.path(sparkHome, "R", "lib")))

  library(SparkR)
}
