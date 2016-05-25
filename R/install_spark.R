spark_install_info <- function(sparkVersion = "1.6.0", hadoopVersion = "2.6") {
  versionInfo <- spark_versions_download_info(sparkVersion, hadoopVersion)

  componentName <- versionInfo$componentName
  packageName <- versionInfo$packageName
  packageSource <- versionInfo$packageSource
  packageRemotePath <- versionInfo$packageRemotePath

  sparkDir <- file.path(getwd(), "spark")
  if (is.installed("rappdirs")) {
    sparkDir <- rappdirs::app_dir("spark", "rstudio")$cache()
  }

  sparkVersionDir <- file.path(sparkDir, componentName)

  list (
    sparkDir = sparkDir,
    packageLocalPath = file.path(sparkDir, packageName),
    packageRemotePath = packageRemotePath,
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
#' @param sparkVersion Version of Spark to install. Suppported versions: "1.6.0" (default), "2.0.0" (preview)
#' @param hadoopVersion Version of Hadoop to install. Supported versions: "2.6" (default), "2.4", "2.3", "1.X", "CDH 4", "user-provided"
#' @param reset Attempts to reset settings to defaults
#' @param logging Logging level to configure install. Supported options: "WARN", "INFO"
spark_install <- function(sparkVersion = "1.6.0", hadoopVersion = "2.6", reset = FALSE, logging = "INFO") {
  installInfo <- spark_install_info(sparkVersion, hadoopVersion)

  if (!dir.exists(installInfo$sparkDir)) {
    dir.create(installInfo$sparkDir, recursive = TRUE)
  }

  if (!file.exists(installInfo$packageLocalPath)) {
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

  invisible(installInfo)
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
