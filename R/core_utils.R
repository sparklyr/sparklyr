core_get_package_function <- function(packageName, functionName) {
  if (
    packageName %in%
      rownames(installed.packages()) &&
      exists(functionName, envir = asNamespace(packageName))
  ) {
    get(functionName, envir = asNamespace(packageName))
  } else {
    NULL
  }
}

# Changing this file requires running update_embedded_sources.R to rebuild sources and jars.

arrow_write_record_batch <- function(df, spark_version_number = NULL) {
  arrow_env_vars <- list()
  if (!is.null(spark_version_number) && spark_version_number < "3.0") {
    # Spark < 3 uses an old version of Arrow, so send data in the legacy format
    arrow_env_vars$ARROW_PRE_0_15_IPC_FORMAT <- 1
  }

  withr::with_envvar(arrow_env_vars, {
    # Set the local timezone to any POSIXt columns that don't have one set
    # https://github.com/sparklyr/sparklyr/issues/2439
    df[] <- lapply(df, function(x) {
      if (inherits(x, "POSIXt") && is.null(attr(x, "tzone"))) {
        attr(x, "tzone") <- Sys.timezone()
      }
      x
    })
    arrow::write_to_raw(df, format = "stream")
  })
}

arrow_record_stream_reader <- function(stream) {
  arrow::RecordBatchStreamReader$create(stream)
}

arrow_read_record_batch <- function(reader) reader$read_next_batch()

arrow_as_tibble <- function(record) as.data.frame(record)

#' Find path to Java
#'
#' Finds the path to \code{JAVA_HOME}.
#'
#' @param throws Throw an error when path not found?
#'
#' @export
#' @keywords internal
spark_get_java <- function(throws = FALSE) {
  java_home <- Sys.getenv("JAVA_HOME", unset = NA)
  if (!is.na(java_home)) {
    java <- file.path(java_home, "bin", "java")
    if (identical(.Platform$OS.type, "windows")) {
      java <- paste0(java, ".exe")
    }
    if (!file.exists(java)) {
      if (throws) {
        stop(
          "Java is required to connect to Spark. ",
          "JAVA_HOME is set to '",
          java_home,
          "' but does not point to a valid version. ",
          "Please fix JAVA_HOME or reinstall from: ",
          java_install_url()
        )
      }
      java <- ""
    }
  } else {
    java <- Sys.which("java")
  }
  java
}

validate_java_version <- function(master, spark_home) {
  # if someone sets SPARK_HOME and we are not in local more, assume Java
  # is available since some systems.
  # (e.g. CDH) use versions of java not discoverable through JAVA_HOME.
  if (
    !spark_master_is_local(master) &&
      !is.null(spark_home) &&
      nchar(spark_home) > 0
  ) {
    return(TRUE)
  }

  # find the active java executable
  java <- spark_get_java(throws = TRUE)
  if (!nzchar(java)) {
    stop(
      "Java is required to connect to Spark. Please download and install Java from ",
      java_install_url()
    )
  }

  # query its version
  version <- system2(java, "-version", stderr = TRUE, stdout = TRUE)
  java_version <- validate_java_version_line(master, version)

  spark_version <- spark_version_from_home(spark_home)
  if (
    compareVersion(java_version, "11") >= 0 &&
      compareVersion(spark_version, "3.0.0") < 0
  ) {
    stop("Java 11 is only supported for Spark 3.0.0+", call. = FALSE)
  }

  TRUE
}

java_is_x64 <- function() {
  java <- spark_get_java(throws = TRUE)
  if (!nzchar(java)) {
    return(FALSE)
  }

  version <- system2(java, "-version", stderr = TRUE, stdout = TRUE)
  any(grepl("64-Bit", version))
}

java_install_url <- function() {
  "https://www.java.com/en/"
}

validate_java_version_line <- function(master, version) {
  if (length(version) < 1) {
    stop(
      "Java version not detected. Please download and install Java from ",
      java_install_url()
    )
  }

  # find line with version info
  versionLine <- version[grepl("version", version)]
  if (length(versionLine) != 1) {
    stop(
      "Java version detected but couldn't parse version from ",
      paste(version, collapse = " - ")
    )
  }

  splatVersion <- if (grepl("openjdk version", versionLine)) {
    strsplit(versionLine, "\"")[[1]][[2]]
  } else {
    splat <- strsplit(versionLine, "\\s+", perl = TRUE)[[1]]
    #Getting rid of dates when present before parsing version from 'java -version'
    splat <- splat[!grepl("[0-9]{4}-[0-9]{2}-[0-9]{2}", splat)]
    splat[grepl("[0-9]{1,2}(\\.[0-9]+\\.[0-9]+)?", splat)]
  }

  if (length(splatVersion) != 1) {
    stop("Java version detected but couldn't parse version from: ", versionLine)
  }

  parsedVersion <- regex_replace(
    splatVersion,
    "^\"|\"$" = "",
    "_" = ".",
    "[^0-9.]+" = ""
  )

  if (!is.character(parsedVersion) || nchar(parsedVersion) < 1) {
    stop("Java version detected but couldn't parse version from: ", versionLine)
  }

  # ensure Java 1.7 or higher
  if (compareVersion(parsedVersion, "1.7") == -1) {
    stop(
      "Java version",
      parsedVersion,
      " detected but 1.7+ is required. Please download and install Java from ",
      java_install_url()
    )
  }

  if (
    compareVersion(parsedVersion, "1.9") >= 0 &&
      compareVersion(parsedVersion, "11") == -1 &&
      spark_master_is_local(master) &&
      !getOption("sparklyr.java9", FALSE)
  ) {
    stop(
      "Java 9 is currently unsupported in Spark distributions unless you manually install Hadoop 2.8 ",
      "and manually configure Spark. Please consider uninstalling Java 9 and reinstalling Java 8. ",
      "To override this failure set 'options(sparklyr.java9 = TRUE)'."
    )
  }

  parsedVersion
}

#' Check whether the connection is open
#'
#' @param sc \code{spark_connection}
#'
#' @keywords internal
#'
#' @export
connection_is_open <- function(sc) {
  UseMethod("connection_is_open")
}

#' A helper function to retrieve values from \code{spark_config()}
#'
#' @param config The configuration list from \code{spark_config()}
#' @param name The name of the configuration entry
#' @param default The default value to use when entry is not present
#'
#' @keywords internal
#' @export
spark_config_value <- function(config, name, default = NULL) {
  if (
    getOption("sparklyr.test.enforce.config", FALSE) &&
      any(grepl("^sparklyr.", name))
  ) {
    settings <- get("spark_config_settings")()
    if (
      !any(name %in% settings$name) &&
        !grepl("^sparklyr\\.shell\\.", name)
    ) {
      stop(
        "Config value '",
        name[[1]],
        "' not described in spark_config_settings()"
      )
    }
  }

  name_exists <- name %in% names(config)
  if (!any(name_exists)) {
    name_exists <- name %in% names(options())
    if (!any(name_exists)) {
      value <- default
    } else {
      name_primary <- name[name_exists][[1]]
      value <- getOption(name_primary)
    }
  } else {
    name_primary <- name[name_exists][[1]]
    value <- config[[name_primary]]
  }

  if (is.language(value)) {
    value <- rlang::as_closure(value)
  }
  if (is.function(value)) {
    value <- value()
  }
  value
}

spark_config_integer <- function(config, name, default = NULL) {
  as.integer(spark_config_value(config, name, default))
}

spark_config_logical <- function(config, name, default = NULL) {
  as.logical(spark_config_value(config, name, default))
}

worker_config_serialize <- function(config) {
  paste(
    if (isTRUE(config$debug)) "TRUE" else "FALSE",
    spark_config_value(config, "sparklyr.worker.gateway.port", "8880"),
    spark_config_value(config, "sparklyr.worker.gateway.address", "localhost"),
    if (isTRUE(config$profile)) "TRUE" else "FALSE",
    if (isTRUE(config$schema)) "TRUE" else "FALSE",
    if (isTRUE(config$arrow)) "TRUE" else "FALSE",
    if (isTRUE(config$fetch_result_as_sdf)) "TRUE" else "FALSE",
    if (isTRUE(config$single_binary_column)) "TRUE" else "FALSE",
    if (isTRUE(config$spark_read)) "TRUE" else "FALSE",
    config$spark_version,
    sep = ";"
  )
}

worker_config_deserialize <- function(raw) {
  parts <- strsplit(raw, ";")[[1]]
  list(
    debug = as.logical(parts[[1]]),
    sparklyr.gateway.port = as.integer(parts[[2]]),
    sparklyr.gateway.address = parts[[3]],
    profile = as.logical(parts[[4]]),
    schema = as.logical(parts[[5]]),
    arrow = as.logical(parts[[6]]),
    fetch_result_as_sdf = as.logical(parts[[7]]),
    single_binary_column = as.logical(parts[[8]]),
    spark_read = as.logical(parts[[9]]),
    spark_version = parts[[10]]
  )
}
