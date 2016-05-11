#' @export
#' @import rappdirs
#' @rdname spark-connection
#' Provides support to download and install the given Spark version
#' @examples
#' spark::spark_install()
spark_install <- function(version) {
  componentName <- paste("spark-", version, "-bin-hadoop2.6", sep = "")

  packageName <- paste(componentName, ".tgz", sep = "")
  packageSource <- "http://d3kbcqa49mib13.cloudfront.net"

  sparkDir <- file.path(getwd(), "spark")
  if (is.installed("rappdirs")) {
    sparkDir <- rappdirs::app_dir("spark", "rstudio")$cache()
  }

  if (!dir.exists(sparkDir)) {
    print("Local spark directory for this project not found, creating.")
    dir.create(sparkDir)
  }

  packagePath <- file.path(sparkDir, packageName)

  if (!file.exists(packagePath)) {
    print("Spark package not found, downloading.")
    download.file(file.path(packageSource, packageName), destfile = packagePath)
  }

  sparkVersionDir <- file.path(sparkDir, componentName)

  if (!dir.exists(sparkVersionDir)) {
    untar(tarfile = packagePath, exdir = sparkDir)
  }

  list (
    sparkDir = sparkDir,
    sparkVersionDir = sparkVersionDir
  )
}

#' @export
#' @rdname spark-connection
connect <- function(master = "local", appName = "rspark") {
  setup_local()
  spark_api_start(master, appName)
}

#' @export
#' @rdname spark-connection
connection_log <- function(con, n = 10) {
  log <- file(con$outputFile)
  lines <- readLines(log)
  close(log)

  lines <- tail(lines, n = n)

  cat(paste(lines, collapse = "\n"))
}

#' @export
#' @rdname spark-connection
connection_ui <- function(con) {
  log <- file(con$outputFile)
  lines <- readLines(log)
  close(log)

  lines <- head(lines, n = 200)

  ui_line <- grep("Started SparkUI at ", lines, perl=TRUE, value=TRUE)
  if (length(ui_line) > 0) {
    matches <- regexpr("http://.*", ui_line, perl=TRUE)
    match <-regmatches(ui_line, matches)
    if (length(match) > 0) {
      browseURL(match)
    }
  }
}
