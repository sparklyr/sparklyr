download_spark <- function(version) {
  componentName <- paste("spark-", version, "-bin-without-hadoop", sep = "")
  packageName <- paste(componentName, ".tgz", sep = "")
  packageSource <- "http://d3kbcqa49mib13.cloudfront.net"

  if (!dir.exists("spark")) {
    warning("Local spark directory for this project not found, creating.")
    dir.create("spark")
  }

  sparkDir <- file.path(getwd(), "spark")
  packagePath <- file.path(sparkDir, packageName)

  if (!file.exists(packagePath)) {
    warning("Spark package not found, downloading.")
    download.file(file.path(packageSource, packageName), destfile = packagePath)
  }

  extractionPath <- file.path(sparkDir, componentName)

  if (!dir.exists(extractionPath)) {
    untar(tarfile = packagePath, exdir = sparkDir)
  }

  return (extractionPath)
}
