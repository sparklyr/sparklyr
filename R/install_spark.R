

#' @rdname spark_install
#' @export
spark_install_dir <- function() {
  getOption("spark.install.dir", rappdirs::app_dir("spark", "rstudio")$cache())
}

# Check if Spark can be installed in this system
spark_can_install <- function() {
  sparkDir <- spark_install_dir()
  if (dir.exists(sparkDir))
    file.access(sparkDir, 2) == 0
  else
    TRUE
}

# Check if the given Spark version is available in this system
spark_install_available <- function(version, hadoop_version) {
  installInfo <- spark_versions_info(version, hadoop_version)
  dir.exists(installInfo$sparkVersionDir)
}

spark_install_find <- function(sparkVersion = NULL, hadoopVersion = NULL, installedOnly = TRUE, latest = FALSE) {
  versions <- spark_versions(latest = latest)
  if (installedOnly)
    versions <- versions[versions$installed, ]
  versions <- if (is.null(sparkVersion)) versions else versions[versions$spark == sparkVersion, ]
  versions <- if (is.null(hadoopVersion)) versions else versions[versions$hadoop == hadoopVersion, ]

  if(NROW(versions) == 0) {
    sparkInstall <- quote(spark_install(version = "", hadoop_version = ""))
    sparkInstall$version <- sparkVersion
    sparkInstall$hadoop_version <- hadoopVersion

    stop(paste("Spark version not installed. To install, use ", deparse(sparkInstall)))
  }

  versions <- versions[with(versions, order(-default, -hadoop_default)), ]
  spark_install_info(as.character(versions[1,]$spark), as.character(versions[1,]$hadoop))
}

spark_install_info <- function(sparkVersion = NULL, hadoopVersion = NULL) {
  versionInfo <- spark_versions_info(sparkVersion, hadoopVersion)

  componentName <- versionInfo$componentName
  packageName <- versionInfo$packageName
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

#' Download and install various versions of Spark
#' 
#' @param version Version of Spark to install. See \code{spark_available_versions} for a list of supported versions
#' @param hadoop_version Version of Hadoop to install. See \code{spark_available_versions} for a list of supported versions
#' @param reset Attempts to reset settings to defaults
#' @param logging Logging level to configure install. Supported options: "WARN", "INFO"
#' @param verbose Report information as Spark is downloaded / installed?
#' @param tarfile Path to TAR file conforming to the pattern spark-###-bin-hadoop### where ###
#' reference spark and hadoop versions respectevely.
#' @export
spark_install <- function(version = NULL,
                          hadoop_version = NULL,
                          reset = FALSE,
                          logging = "INFO",
                          verbose = interactive())
{
  installInfo <- spark_install_find(version, hadoop_version, installedOnly = FALSE, latest = TRUE)

  if (!dir.exists(installInfo$sparkDir)) {
    dir.create(installInfo$sparkDir, recursive = TRUE)
  }

  if (!dir.exists(installInfo$sparkVersionDir)) {

    if (verbose) {

      fmt <- paste(c(
        "Installing Spark %s for Hadoop %s or later.",
        "Downloading from:\n- '%s'",
        "Installing to:\n- '%s'"
      ), collapse = "\n")

      msg <- sprintf(fmt,
                     installInfo$sparkVersion,
                     installInfo$hadoopVersion,
                     installInfo$packageRemotePath,
                     aliased_path(installInfo$sparkVersionDir))

      message(msg)
    }

    download.file(
      installInfo$packageRemotePath,
      destfile = installInfo$packageLocalPath,
      quiet = !verbose
    )

    untar(tarfile = installInfo$packageLocalPath, exdir = installInfo$sparkDir)
    unlink(installInfo$packageLocalPath)

    if (verbose)
      message("Installation complete.")

  } else if (verbose) {
    fmt <- "Spark %s for Hadoop %s or later already installed."
    msg <- sprintf(fmt, installInfo$sparkVersion, installInfo$hadoopVersion)
    message(msg)
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

  hiveSitePath <- file.path(installInfo$sparkConfDir, "hive-site.xml")
  if (!file.exists(hiveSitePath) || reset) {
    hiveSiteTemplatePath <- system.file(package = "sparklyr", file.path("conf", "hive-site.xml"))
    file.copy(hiveSiteTemplatePath, hiveSitePath, overwrite = TRUE)
  }

  invisible(installInfo)
}

#' @rdname spark_install
#' @export
spark_install_tar <- function(tarfile) {
  if (!file.exists(tarfile)) {
    stop(paste0("The file \"", tarfile, "\", does not exist."))
  }

  filePattern <- spark_versions_file_pattern();
  fileName <- basename(tarfile)
  if (length(grep(filePattern, fileName)) == 0) {
    stop(paste(
      "The given file does not conform with the following pattern: ", filePattern))
  }

  untar(tarfile = tarfile, exdir = spark_install_dir())
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
