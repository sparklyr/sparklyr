#' Retrieve Available Settings
#'
#' Retrieves available sparklyr settings that can be used in configuration files or \code{spark_config()}.
#'
#' @export
spark_config_settings <- function() {
  settings <- list(
    sparklyr.apply.packages = "Configures default value for packages parameter in spark_apply().",
    sparklyr.apply.rlang = "Experimental feature. Turns on improved serialization for spark_apply().",
    sparklyr.apply.serializer = "Configures the version spark_apply() uses to serialize the closure.",
    sparklyr.apply.schema.infer = "Number of rows collected to infer schema when column types specified in spark_apply().",
    sparklyr.arrow = "Use Apache Arrow to serialize data?",
    sparklyr.backend.interval = "Total seconds sparklyr will check on a backend operation.",
    sparklyr.backend.timeout = "Total seconds before sparklyr will give up waiting for a backend operation to complete.",
    sparklyr.collect.batch = "Total rows to collect when using batch collection, defaults to 100,000.",
    sparklyr.cancellable = "Cancel spark jobs when the R session is interrupted?",
    sparklyr.connect.aftersubmit = "R function to call after spark-submit executes.",
    sparklyr.connect.app.jar = "The path to the sparklyr jar used in spark_connect().",
    sparklyr.connect.cores.local = "Number of cores to use in spark_connect(master = \"local\"), defaults to parallel::detectCores().",
    sparklyr.connect.csv.embedded = "Regular expression to match against versions of Spark that require package extension to support CSVs.",
    sparklyr.connect.csv.scala11 = "Use Scala 2.11 jars when using embedded CSV jars in Spark 1.6.X.",
    sparklyr.connect.enablehivesupport = "Whether to enable Hive integration with Spark (default: true)",
    sparklyr.connect.jars = "Additional JARs to include while submitting application to Spark.",
    sparklyr.connect.master = "The cluster master as spark_connect() master parameter, notice that the 'spark.master' setting is usually preferred.",
    sparklyr.connect.packages = "Spark packages to include when connecting to Spark.",
    sparklyr.connect.ondisconnect = "R function to call after spark_disconnect().",
    sparklyr.connect.sparksubmit = "Command executed instead of spark-submit when connecting.",
    sparklyr.connect.timeout = "Total seconds before giving up connecting to the sparklyr gateway while initializing.",
    sparklyr.dbplyr.edition = "Edition of dbplyr API to use with spark connections.",
    sparklyr.dplyr.period.splits = "Should 'dplyr' split column names into database and table?",
    sparklyr.extensions.catalog = "Catalog PATH where extension JARs are located. Defaults to 'TRUE', 'FALSE' to disable.",
    sparklyr.gateway.address = "The address of the driver machine.",
    sparklyr.gateway.config.retries = "Number of retries to retrieve port and address from config, useful when using functions to query port or address in kubernetes.",
    sparklyr.gateway.interval = "Total of seconds sparkyr will check on a gateway connection.",
    sparklyr.gateway.port = "The port the sparklyr gateway uses in the driver machine, defaults to 8880.",
    sparklyr.gateway.port.query.attempts = "Max number of attempts to query Sparklyr gateway ports.",
    sparklyr.gateway.port.query.retry.interval.seconds = "Number of seconds between two consecutive Sparklyr gateway ports query attempts.",
    sparklyr.gateway.remote = "Should the sparklyr gateway allow remote connections? This is required in yarn cluster, etc.",
    sparklyr.gateway.routing = "Should the sparklyr gateway service route to other sessions? Consider disabling in kubernetes.",
    sparklyr.gateway.service = "Should the sparklyr gateway be run as a service without shutting down when the last connection disconnects?",
    sparklyr.gateway.timeout = "Total seconds before giving up connecting to the sparklyr gateway after initialization.",
    sparklyr.gateway.wait = "Total seconds to wait before retrying to contact the sparklyr gateway.",
    sparklyr.livy.auth = "Authentication method for Livy connections.",
    sparklyr.livy.branch = "GitHub branch to be used to deploy sparklyr JAR to Livy.",
    sparklyr.livy.jar = "URL or path to the sparklyr JAR to be used by Livy.",
    sparklyr.livy.headers = "Additional HTTP headers for Livy connections.",
    sparklyr.livy.proxy = "Optional HTTP proxy setting for Livy connections (any non-default value must be specified via httr::use_proxy()).",
    sparklyr.livy.additional_curl_opts = "Additional CURL options for Livy connections (see httr::httr_options() for a list of valid options).",
    sparklyr.livy_create_session.retries = "Number of retries when creating Livy session",
    sparklyr.livy_create_session.retry_interval_s = "Retry interval (in seconds) when creating Livy session",
    sparklyr.log.invoke = "Should every call to invoke() be printed in the console? Can be set to 'callstack' to log call stack.",
    sparklyr.log.console = "Should driver logs be printed in the console?",
    sparklyr.progress = "Should job progress be reported to RStudio?",
    sparklyr.progress.interval = "Total of seconds to wait before attempting to retrieve job progress in Spark.",
    sparklyr.sanitize.column.names = "Should partially unsupported column names be cleaned up?",
    sparklyr.stream.collect.timeout = "Total seconds before stopping collecting a stream sample in sdf_collect_stream().",
    sparklyr.stream.validate.timeout = "Total seconds before stopping to check if stream has errors while being created.",
    sparklyr.verbose = "Use verbose logging across all sparklyr operations?",
    sparklyr.verbose.na = "Use verbose logging when dealing with NAs?",
    sparklyr.verbose.sanitize = "Use verbose logging while sanitizing columns and other objects?",
    sparklyr.web.spark = "The URL to Spark's web interface.",
    sparklyr.web.yarn = "The URL to YARN's web interface.",
    sparklyr.worker.gateway.address = "The address of the worker machine, most likely localhost.",
    sparklyr.worker.gateway.port = "The port the sparklyr gateway uses in the driver machine.",
    sparklyr.yarn.cluster.accepted.timeout = "Total seconds before giving up waiting for cluster resources in yarn cluster mode.",
    sparklyr.yarn.cluster.hostaddress.timeout = "Total seconds before giving up waiting for the cluster to assign a host address in yarn cluster mode.",
    sparklyr.yarn.cluster.lookup.byname = "Should the current user name be used to filter yarn cluster jobs while searching for submitted one?",
    sparklyr.yarn.cluster.lookup.prefix = "Application name prefix used to filter yarn cluster jobs while searching for submitted one.",
    sparklyr.yarn.cluster.lookup.username = "The user name used to filter yarn cluster jobs while searching for submitted one.",
    sparklyr.yarn.cluster.start.timeout = "Total seconds before giving up waiting for yarn cluster application to get registered."
  )

  data.frame(
    name = names(settings),
    description = unlist(unname(settings))
  )
}

#' Read Spark Configuration
#'
#' Read Spark Configuration
#'
#' @include arrow.R

#' Read Spark Configuration
#'
#' @export
#' @param file Name of the configuration file
#' @param use_default TRUE to use the built-in defaults provided in this package
#'
#' @details
#'
#' Read Spark configuration using the \pkg{\link[config]{config}} package.
#'
#' @return Named list with configuration data
spark_config <- function(file = "config.yml", use_default = TRUE) {
  baseConfig <- list()

  if (use_default) {
    localConfigFile <- system.file(
      file.path("conf", "config-template.yml"),
      package = "sparklyr"
    )
    baseConfig <- config::get(file = localConfigFile)
  }

  # allow options to specify sparklyr configuration settings
  optionsConfigCheck <- grepl(
    "^spark\\.|^sparklyr\\.|^livy\\.",
    names(options())
  )
  optionsConfig <- options()[optionsConfigCheck]
  baseConfig <- merge_lists(optionsConfig, baseConfig)

  userEnvConfig <- tryCatch(
    config::get(file = Sys.getenv("SPARKLYR_CONFIG_FILE")),
    error = function(e) NULL
  )
  baseEnvConfig <- merge_lists(baseConfig, userEnvConfig)

  isFileProvided <- !missing(file)
  userConfig <- tryCatch(
    config::get(file = file),
    error = function(e) {
      if (isFileProvided) {
        warnMessage <- sprintf(
          "Error reading config file: %s in spark_config(): %s: %s. File will be ignored.",
          file,
          deparse(e[["call"]]),
          e[["message"]]
        )
        warning(warnMessage, call. = FALSE)
      }
      NULL
    }
  )

  mergedConfig <- merge_lists(baseEnvConfig, userConfig)

  if (
    nchar(Sys.getenv("SPARK_DRIVER_CLASSPATH")) > 0 &&
      is.null(mergedConfig$master$`sparklyr.shell.driver-class-path`)
  ) {
    mergedConfig$master$`sparklyr.shell.driver-class-path` <- Sys.getenv(
      "SPARK_DRIVER_CLASSPATH"
    )
  }

  if (
    is.null(spark_config_value(
      mergedConfig,
      c("sparklyr.cores.local", "sparklyr.connect.cores.local")
    ))
  ) {
    mergedConfig$sparklyr.connect.cores.local <- parallel::detectCores()
  }

  if (
    is.null(spark_config_value(
      mergedConfig,
      "spark.sql.shuffle.partitions.local"
    ))
  ) {
    mergedConfig$spark.sql.shuffle.partitions.local <- parallel::detectCores()
  }

  mergedConfig
}

#' A helper function to check value exist under \code{spark_config()}
#'
#' @param config The configuration list from \code{spark_config()}
#' @param name The name of the configuration entry
#' @param default The default value to use when entry is not present
#'
#' @keywords internal
#' @export
spark_config_exists <- function(config, name, default = NULL) {
  if (!name %in% names(config)) default else !identical(config[[name]], FALSE)
}

# recursively merge two lists -- extracted from code used by rmarkdown
# package to merge _output.yml, _site.yml, front matter, etc.:
# https://github.com/rstudio/rmarkdown/blob/master/R/util.R#L174
merge_lists <- function(base_list, overlay_list, recursive = TRUE) {
  if (length(base_list) == 0) {
    overlay_list
  } else if (length(overlay_list) == 0) {
    base_list
  } else {
    merged_list <- base_list
    for (name in names(overlay_list)) {
      base <- base_list[[name]]
      overlay <- overlay_list[[name]]
      if (is.list(base) && is.list(overlay) && recursive) {
        merged_list[[name]] <- merge_lists(base, overlay)
      } else {
        merged_list[[name]] <- NULL
        merged_list <- append(
          merged_list,
          overlay_list[which(names(overlay_list) %in% name)]
        )
      }
    }
    merged_list
  }
}

spark_config_value_retries <- function(config, name, default, retries) {
  success <- FALSE
  value <- default

  while (!success && retries > 0) {
    retries <- retries - 1

    result <- tryCatch(
      {
        list(
          value = spark_config_value(config, name, default),
          success = TRUE
        )
      },
      error = function(e) {
        if (spark_config_value(config, "sparklyr.verbose", FALSE)) {
          message("Reading ", name, " failed with error: ", e$message)
        }

        if (retries > 0) {
          Sys.sleep(1)
        }

        list(
          success = FALSE
        )
      }
    )

    success <- result$success
    value <- result$value
  }

  if (!success) {
    stop("Failed after ", retries, " attempts while reading conf value ", name)
  }

  value
}

#' Creates Spark Configuration
#'
#' @param config The Spark configuration object.
#' @param packages A list of named packages or versioned packagese to add.
#' @param version The version of Spark being used.
#' @param scala_version Acceptable Scala version of packages to be loaded
#' @param ... Additional configurations
#'
#' @keywords internal
#' @export
spark_config_packages <- function(
  config,
  packages,
  version,
  scala_version = NULL,
  ...
) {
  version <- spark_version_latest(version)

  scala_version <- scala_version %||%
    (if (version >= "4.0.0") {
      "2.13"
    } else if (version >= "3.0.0") {
      "2.12"
    } else {
      "2.11"
    })

  if ("kafka" %in% packages) {
    packages <- packages[-which(packages == "kafka")]

    if (version < "2.0.0") {
      stop("Kafka requires Spark 2.x")
    }

    kafka_package <- sprintf(
      "org.apache.spark:spark-sql-kafka-0-10_%s:",
      scala_version
    )
    kafka_package <- paste0(kafka_package, version)

    config$sparklyr.shell.packages <- c(
      config$sparklyr.shell.packages,
      kafka_package
    )
  }

  if ("delta" %in% packages) {
    packages <- packages[-which(packages == "delta")]

    if (version < "2.4.2") {
      stop("Delta Lake requires Spark 2.4.2 or newer")
    }

    delta <- list(
      list(spark = "2.4", delta = "0.6.0"),
      list(spark = "3.0", delta = "0.8.0"),
      list(spark = "3.1", delta = "1.0.1"),
      list(spark = "3.2", delta = "2.0.2"),
      list(spark = "3.3", delta = "2.3.0"),
      list(spark = "3.4", delta = "2.4.0"),
      list(spark = "3.5", delta = "3.0.0"),
      list(spark = "4.0", delta = "4.0.0")
    ) %>%
      purrr::keep(~ .x$spark >= substr(version, 1, 3)) %>%
      head(1) %>%
      unlist()

    if (length(delta) == 0) {
      stop(
        "Delta Lake is not yet available for Spark ",
        version,
        "; the latest delta-spark release targets Spark 4.0."
      )
    }

    delta_version <- delta[2]

    if (version >= "3.5") {
      delta_name <- "delta-spark"
    } else {
      delta_name <- "delta-core"
    }

    config$sparklyr.shell.packages <- c(
      config$sparklyr.shell.packages,
      sprintf(
        "io.delta:%s_%s:%s",
        delta_name,
        scala_version,
        delta_version
      )
    )
    if (version >= 3.3) {
      config$`spark.sql.extensions` <- "io.delta.sql.DeltaSparkSessionExtension"
      config$`spark.sql.catalog.spark_catalog` <- "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    }
  }

  if ("avro" %in% packages) {
    packages <- packages[-which(packages == "avro")]

    if (is.null(version)) {
      stop(
        "`package = \"avro\")` requires Spark version to be specified via ",
        "`spark_connect(..., version = <Spark version>)`"
      )
    }

    config$sparklyr.shell.packages <- c(
      config$sparklyr.shell.packages,
      spark_avro_package_name(version, scala_version)
    )
  }

  if ("rapids" %in% packages) {
    packages <- packages[-which(packages == "rapids")]

    if (version < "3.0.0") {
      stop("RAPIDS library requires Spark 3.0.0 or higher")
    }

    additional_configs <- list(...)
    config$sparklyr.shell.packages <- c(
      config$sparklyr.shell.packages,
      (if (
        additional_configs$method %in% c("databricks", "databricks-connect")
      ) {
        "com.nvidia:rapids-4-spark_2.12:0.1.0-databricks"
      } else {
        "com.nvidia:rapids-4-spark_2.12:0.1.0"
      }),
      "ai.rapids:cudf:0.14"
    )

    rapids_prefix <- "spark.rapids."
    rapids_configs <- connection_config(
      sc = list(config = config),
      prefix = rapids_prefix
    )
    rapids_configs[["sql.incompatibleOps.enabled"]] <-
      rapids_configs[["sql.incompatibleOps.enabled"]] %||% "true"
    config <- append(
      config,
      list(sparklyr.shell.conf = "spark.plugins=com.nvidia.spark.SQLPlugin")
    )
    for (idx in seq_along(rapids_configs)) {
      k <- names(rapids_configs)[[idx]]
      v <- rapids_configs[[idx]]
      config <- append(
        config,
        list(sparklyr.shell.conf = paste0(rapids_prefix, k, "=", v))
      )
    }
  }

  if (!is.null(packages)) {
    config$sparklyr.shell.packages <- c(
      config$sparklyr.shell.packages,
      packages
    )
  }

  config
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
