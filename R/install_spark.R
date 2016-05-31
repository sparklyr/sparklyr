spark_install_dir <- function() {
  sparkDir <- getOption("rspark.install.dir", NULL)
  if (is.null(sparkDir)) {
    sparkDir <- rappdirs::app_dir("spark", "rstudio")$cache()
  }

  sparkDir
}

#' Check if Spark can be installed in this system
#' @name spark_can_install
spark_can_install <- function() {
  sparkDir <- spark_install_dir()
  dir.exists(sparkDir) && file.access(sparkDir, 2) == 0
}

#' Check if the given Spark version is available in this system
#' @name spark_install_available
#' @param version Version of Spark to install. See spark_versions.
#' @param hadoop_version Version of Spark to install. See spark_versions_hadoop.
spark_install_available <- function(version, hadoop_version) {
  installInfo <- spark_versions_info(version, hadoop_version)
  dir.exists(installInfo$sparkVersionDir)
}

spark_install_find <- function(sparkVersion = NULL, hadoopVersion = NULL) {
  versions <- spark_versions_df()
  versions <- versions[versions$installed, ]
  versions <- if (is.null(sparkVersion)) versions else versions[versions$spark == sparkVersion, ]
  versions <- if (is.null(hadoopVersion)) versions else versions[versions$hadoop == hadoopVersion, ]

  if(NROW(versions) == 0) {
    sparkInstall <- quote(spark_install(version = "", hadoopVersion = ""))
    sparkInstall[[2]] <- sparkVersion
    sparkInstall[[3]] <- hadoopVersion

    stop(paste("Spark version not installed. To install, use ", deparse(sparkInstall)))
  }

  spark_install_info(as.character(versions[1,]$spark), as.character(versions[1,]$hadoop))
}

spark_install_info <- function(sparkVersion = NULL, hadoopVersion = NULL) {
  versionInfo <- spark_versions_info(sparkVersion, hadoopVersion)

  componentName <- versionInfo$componentName
  packageName <- versionInfo$packageName
  packageSource <- versionInfo$packageSource
  packageRemotePath <- versionInfo$packageRemotePath

  sparkDir <- spark_install_dir()
  sparkVersionDir <- file.path(sparkDir, componentName)

  list (
    sparkDir = sparkDir,
    packageLocalPath = file.path(sparkDir, packageName),
    packageRemotePath = packageRemotePath,
    sparkVersionDir = sparkVersionDir,
    sparkConfDir = file.path(sparkVersionDir, "conf"),
    sparkVersion = sparkVersion,
    hadoopVersion = hadoopVersion,
    installed = file.exists(sparkVersionDir)
  )
}

#' Validates that the given Spark version has been downloaded and installed locally
#' @name spark_check_install
#' @export
#' @import rappdirs
#' @param version Version of Spark to install. See spark_versions.
#' @param hadoop_version Version of Spark to install. See spark_versions_hadoop.
spark_check_install <- function(version = "1.6.0", hadoop_version = "2.6") {
  installInfo <- spark_install_info(version, hadoop_version)

  if (!file.exists(installInfo$sparkDir)) {
    stop("Spark version not found. Install with spark_install.")
  }

  installInfo
}

#' Provides support to download and install the given Spark version
#' @name spark_install
#' @export
#' @import rappdirs
#' @param version Version of Spark to install. See spark_versions for a list of supported versions
#' @param hadoop_version Version of Hadoop to install. See spark_versions_hadoop for a list of supported versions
#' @param reset Attempts to reset settings to defaults
#' @param logging Logging level to configure install. Supported options: "WARN", "INFO"
spark_install <- function(version = "1.6.0", hadoop_version = "2.6", reset = FALSE, logging = "INFO") {
  installInfo <- spark_install_info(version, hadoop_version)

  if (!dir.exists(installInfo$sparkDir)) {
    dir.create(installInfo$sparkDir, recursive = TRUE)
  }

  if (!dir.exists(installInfo$sparkVersionDir)) {
    download.file(installInfo$packageRemotePath, destfile = installInfo$packageLocalPath)
    untar(tarfile = installInfo$packageLocalPath, exdir = installInfo$sparkDir)
    unlink(installInfo$packageLocalPath)
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
