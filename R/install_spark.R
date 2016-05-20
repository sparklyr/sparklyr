spark_install_info <- function(version = "1.6.0") {
  versionInfo <- list(
    `1.6.0` = list(
      componentName = paste("spark-", version, "-bin-hadoop2.6", sep = ""),
      packageSource = "http://d3kbcqa49mib13.cloudfront.net"
    ),
    `2.0.0` = list(
      componentName = paste("spark-", version, "-SNAPSHOT-bin-hadoop2.7", sep = ""),
      packageSource = "http://people.apache.org/~pwendell/spark-nightly/spark-master-bin/spark-2.0.0-SNAPSHOT-2016_05_15_01_03-354f8f1-bin/"
    )
  )

  if (!(version %in% names(versionInfo))) {
    stop(paste("The Spark version", version, "is currently not supported"))
  }

  componentName <- versionInfo[[version]]$componentName
  packageName <- paste(componentName, ".tgz", sep = "")
  packageSource <- versionInfo[[version]]$packageSource

  sparkDir <- file.path(getwd(), "spark")
  if (is.installed("rappdirs")) {
    sparkDir <- rappdirs::app_dir("spark", "rstudio")$cache()
  }

  sparkVersionDir <- file.path(sparkDir, componentName)

  list (
    sparkDir = sparkDir,
    packageLocalPath = file.path(sparkDir, packageName),
    packageRemotePath = file.path(packageSource, packageName),
    sparkVersionDir = sparkVersionDir,
    sparkConfDir = file.path(sparkVersionDir, "conf"),
    version = version
  )
}

#' Validates that the given Spark version has been downloaded and installed locally
#' @name spark_check_install
#' @export
#' @import rappdirs
#' @param version Version of Spark to install. Suppported versions: "1.6.0" (default), "2.0.0" (preview)
spark_check_install <- function(version = "1.6.0") {
  installInfo <- spark_install_info(version)

  if (!file.exists(installInfo$sparkDir)) {
    stop("Spark version not found. Install with spark_install.")
  }

  installInfo
}

#' Provides support to download and install the given Spark version
#' @name spark_install
#' @export
#' @import rappdirs
#' @param version Version of Spark to install. Suppported versions: "1.6.0" (default), "2.0.0" (preview)
#' @param reset Attempts to reset settings to defaults
#' @param logging Logging level to configure install. Supported options: "WARN", "INFO"
spark_install <- function(version = "1.6.0", reset = FALSE, logging = "INFO") {
  installInfo <- spark_install_info(version)

  if (!dir.exists(installInfo$sparkDir)) {
    warning("Local spark directory for this project not found, creating.")
    dir.create(installInfo$sparkDir, recursive = TRUE)
  }

  if (!file.exists(installInfo$packageLocalPath)) {
    warning("Spark package not found, downloading.")
    download.file(installInfo$packageRemotePath, destfile = installInfo$packageLocalPath)
  }

  if (!dir.exists(installInfo$sparkVersionDir)) {
    untar(tarfile = installInfo$packageLocalPath, exdir = installInfo$sparkDir)
  }

  if (!file.exists(installInfo$sparkDir)) {
    stop("Spark version not found.")
  }

  if (!identical(logging, NULL)) {
    spark_conf_file_set_value(
      installInfo,
      "log4j.rootCategory",
      paste(logging, "console", sep = ", "),
      reset)
  }

  installInfo
}

spark_conf_file_set_value <- function(installInfo, property, value, reset) {
  log4jPropertiesPath <- file.path(installInfo$sparkConfDir, "log4j.properties")
  if (!file.exists(log4jPropertiesPath) || reset) {
    log4jTemplatePath <- file.path(installInfo$sparkConfDir, "log4j.properties.template")
    file.copy(log4jTemplatePath, log4jPropertiesPath, overwrite = TRUE)
  }

  log4jPropertiesFile <- file(log4jPropertiesPath)
  lines <- readLines(log4jPropertiesFile)

  lines <- gsub(paste(property, ".*", sep = ""), paste(property, value, sep = "="), lines, perl = TRUE)

  writeLines(lines, log4jPropertiesFile)
  close(log4jPropertiesFile)
}
