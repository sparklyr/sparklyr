download_spark <- function() {
  packageName <- "spark-1.6.1-bin-without-hadoop.tgz"
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
}
