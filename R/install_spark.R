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

#' Find a given Spark installation by version.
#'
#' @rdname spark_install
#'
#' @param installed_only Search only the locally installed versions?
#' @param latest Check for latest version?
#' @param hint On failure should the installation code be provided?
#'
#' @keywords internal
#' @export
spark_install_find <- function(version = NULL,
                               hadoop_version = NULL,
                               installed_only = TRUE,
                               latest = FALSE,
                               hint = FALSE) {
  sparkVersion <- version
  hadoopVersion <- hadoop_version
  installedOnly <- installed_only

  versions <- spark_versions(latest = latest)
  if (installedOnly)
    versions <- versions[versions$installed, ]
  versions <- if (is.null(sparkVersion)) versions else versions[versions$spark == sparkVersion, ]
  versions <- if (is.null(hadoopVersion)) versions else versions[versions$hadoop == hadoopVersion, ]

  if(NROW(versions) == 0) {
    if (hint) {
      sparkInstall <- quote(spark_install(version = "", hadoop_version = ""))
      sparkInstall$version <- sparkVersion
      sparkInstall$hadoop_version <- hadoopVersion

      stop(paste("Spark version not installed. To install, use", deparse(sparkInstall)))
    } else {
      stop("Spark version not available. Find available versions, using spark_available_versions()")
    }
  }

  versions <- versions[with(versions, order(default, spark, hadoop_default, decreasing = TRUE)), ]
  spark_install_info(as.character(versions[1,]$spark), as.character(versions[1,]$hadoop), latest = latest)
}

#' determine the version that will be used by default if version is NULL
#'
#' @export
#'
#' @keywords internal
spark_default_version <- function() {
  # if we have versions installed then use the same logic as spark_connect to figure out
  # which version we will bind to when we pass version = NULL and hadoop_version = NULL
  if (nrow(spark_installed_versions()) > 0) {
    version <- spark_install_find(version = NULL, hadoop_version = NULL, installed_only = TRUE, latest = FALSE)
    spark <- version$sparkVersion
    hadoop <- version$hadoopVersion
    # otherwise check available versions and take the default
  } else {
    versions <- spark_versions()
    versions <- subset(versions, versions$default == TRUE & versions$hadoop_default == TRUE)
    version <- versions[1,]
    spark <- version$spark
    hadoop <- version$hadoop
  }

  list(spark = spark,
       hadoop = hadoop)
}

spark_install_info <- function(sparkVersion = NULL, hadoopVersion = NULL, latest = TRUE) {
  versionInfo <- spark_versions_info(sparkVersion, hadoopVersion, latest = latest)

  componentName <- versionInfo$componentName
  packageName <- versionInfo$packageName
  packageRemotePath <- versionInfo$packageRemotePath

  sparkDir <- c(spark_install_old_dir(), spark_install_dir())

  sparkVersionDir <- file.path(sparkDir, componentName)
  sparkDir <- sparkDir[dir.exists(sparkVersionDir)]
  sparkDir <- if (length(sparkDir) == 0) spark_install_dir() else sparkDir[[1]]

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

# Do not remove (it's used by the RStudio IDE)
spark_home <- function() {
  home <- Sys.getenv("SPARK_HOME", unset = NA)
  if (is.na(home))
    home <- NULL
  home
}

#' Download and install various versions of Spark
#'
#' Install versions of Spark for use with local Spark connections
#'   (i.e. \code{spark_connect(master = "local"})
#'
#' @param version Version of Spark to install. See \code{spark_available_versions} for a list of supported versions
#' @param hadoop_version Version of Hadoop to install. See \code{spark_available_versions} for a list of supported versions
#' @param reset Attempts to reset settings to defaults.
#' @param logging Logging level to configure install. Supported options: "WARN", "INFO"
#' @param verbose Report information as Spark is downloaded / installed
#' @param tarfile Path to TAR file conforming to the pattern spark-###-bin-(hadoop)?### where ###
#' reference spark and hadoop versions respectively.
#'
#' @return List with information about the installed version.
#'
#' @import utils
#' @export
spark_install <- function(version = NULL,
                          hadoop_version = NULL,
                          reset = TRUE,
                          logging = "INFO",
                          verbose = interactive())
{
  installInfo <- spark_install_find(version, hadoop_version, installed_only = FALSE, latest = TRUE)

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

    status <- suppressWarnings(download.file(
      installInfo$packageRemotePath,
      destfile = installInfo$packageLocalPath,
      quiet = !verbose
    ))

    if (status)
      stopf("Failed to download Spark: download exited with status %s", status)

    untar(tarfile = installInfo$packageLocalPath,
          exdir = installInfo$sparkDir,
          tar = "internal")

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
    tryCatch({
      spark_conf_log4j_set_value(
        installInfo,
        list(
          "log4j.rootCategory" = paste0("log4j.rootCategory=", logging, ",console,localfile"),
          "log4j.appender.localfile" = "log4j.appender.localfile=org.apache.log4j.DailyRollingFileAppender",
          "log4j.appender.localfile.file" = "log4j.appender.localfile.file=logs/log4j.spark.log",
          "log4j.appender.localfile.layout" = "log4j.appender.localfile.layout=org.apache.log4j.PatternLayout",
          "log4j.appender.localfile.layout.ConversionPattern" = "log4j.appender.localfile.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n"),
        reset)
    }, error = function(e) {
      warning("Failed to set logging settings")
    })
  }

  hiveSitePath <- file.path(installInfo$sparkConfDir, "hive-site.xml")
  hivePath <- NULL
  if (!file.exists(hiveSitePath) || reset) {
    tryCatch({
      hiveProperties <- list(
        "javax.jdo.option.ConnectionURL" = "jdbc:derby:memory:databaseName=metastore_db;create=true",
        "javax.jdo.option.ConnectionDriverName" = "org.apache.derby.jdbc.EmbeddedDriver"
      )

      if (.Platform$OS.type == "windows") {
        hivePath <- normalizePath(file.path(installInfo$sparkVersionDir, "tmp", "hive"), mustWork = FALSE, winslash = "/")

        hiveProperties <- c(hiveProperties, list(
          "hive.exec.scratchdir" = hivePath,
          "hive.exec.local.scratchdir" = hivePath,
          "hive.metastore.warehouse.dir" = hivePath
        ))
      }

      spark_hive_file_set_value(hiveSitePath, hiveProperties)
    }, error = function(e) {
      warning("Failed to apply custom hive-site.xml configuration")
    })
  }

  if (!is.null(hivePath)) {
    tryCatch({
      spark_conf_file_set_value(
        installInfo,
        list(
          "spark.sql.warehouse.dir" = paste0("spark.sql.warehouse.dir          ", hivePath)
        ),
        reset
      )
    }, error = function(e) {
      warning("Failed to set spark-defaults.conf settings")
    })
  }

  invisible(installInfo)
}

#' @rdname spark_install
#'
#' @export
spark_uninstall <- function(version, hadoop_version) {
  info <- spark_versions_info(version, hadoop_version)
  sparkDir <- file.path(c(spark_install_old_dir(), spark_install_dir()), info$componentName)
  if (any(dir.exists(sparkDir))) {
    unlink(sparkDir, recursive = TRUE)
    message(info$componentName, " successfully uninstalled.")
    invisible(TRUE)
  } else {
    message(info$componentName, " not found (no uninstall performed)")
    invisible(FALSE)
  }
}

spark_resolve_envpath <- function(path_with_end) {
  if (.Platform$OS.type == "windows") {
    parts <- strsplit(path_with_end, "/")[[1]]
    first <- gsub("%", "", parts[[1]])
    if (nchar(Sys.getenv(first)) > 0) parts[[1]] <- Sys.getenv(first)
    do.call("file.path", as.list(parts))
  }
  else {
    normalizePath(path_with_end, mustWork = FALSE)
  }
}

#' @rdname spark_install
#' @importFrom jsonlite fromJSON
#' @export
spark_install_dir <- function() {
  config <- fromJSON(
    system.file("extdata/config.json", package = packageName())
  )

  getOption("spark.install.dir", spark_resolve_envpath(config$dirs[[.Platform$OS.type]]))
}

# Used for backwards compatibility with sparklyr 0.5 installation path
spark_install_old_dir <- function() {
  getOption("spark.install.dir", rappdirs::app_dir("spark", "rstudio")$cache())
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

  untar(tarfile = tarfile,
        exdir = spark_install_dir(),
        tar = "internal")
}

spark_conf_log4j_set_value <- function(installInfo, properties, reset) {
  log4jPropertiesPath <- file.path(installInfo$sparkConfDir, "log4j.properties")
  if (!file.exists(log4jPropertiesPath) || reset) {
    log4jTemplatePath <- file.path(installInfo$sparkConfDir, "log4j.properties.template")
    file.copy(log4jTemplatePath, log4jPropertiesPath, overwrite = TRUE)
  }

  log4jPropertiesFile <- file(log4jPropertiesPath)
  lines <- readLines(log4jPropertiesFile)

  lines[[length(lines) + 1]] <- ""

  lapply(names(properties), function(property) {
    value <- properties[[property]]
    pattern <- paste(property, "=.*", sep = "")

    if (length(grep(pattern, lines)) > 0) {
      lines <<- gsub(pattern, value, lines, perl = TRUE)
    }
    else {
      lines[[length(lines) + 1]] <<- value
    }
  })

  writeLines(lines, log4jPropertiesFile)
  close(log4jPropertiesFile)
}

spark_hive_file_set_value <- function(hivePath, properties) {
  lines <- list()
  lines[[length(lines) + 1]] <- "<configuration>"

  lapply(names(properties), function(property) {
    value <- properties[[property]]

    lines[[length(lines) + 1]] <<- "  <property>"
    lines[[length(lines) + 1]] <<- paste0("    <name>", property, "</name>")
    lines[[length(lines) + 1]] <<- paste0("    <value>", value, "</value>")
    lines[[length(lines) + 1]] <<- "  </property>"
  })

  lines[[length(lines) + 1]] <- "</configuration>"

  hiveFile <- file(hivePath)
  writeLines(unlist(lines), hiveFile)
  close(hiveFile)
}

spark_conf_file_set_value <- function(installInfo, properties, reset) {
  confPropertiesPath <- file.path(installInfo$sparkConfDir, "spark-defaults.conf")
  if (!file.exists(confPropertiesPath) || reset) {
    confTemplatePath <- file.path(installInfo$sparkConfDir, "spark-defaults.conf.template")
    file.copy(confTemplatePath, confPropertiesPath, overwrite = TRUE)
  }

  confPropertiesFile <- file(confPropertiesPath)
  lines <- readLines(confPropertiesFile)

  lines[[length(lines) + 1]] <- ""

  lapply(names(properties), function(property) {
    value <- properties[[property]]
    pattern <- paste(property, ".*", sep = "")

    if (length(grep(pattern, lines)) > 0) {
      lines <<- gsub(pattern, value, lines, perl = TRUE)
    }
    else {
      lines[[length(lines) + 1]] <<- value
    }
  })

  writeLines(lines, confPropertiesFile)
  close(confPropertiesFile)
}
