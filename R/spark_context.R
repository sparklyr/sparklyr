#' Runtime configuration interface for the Spark Context.
#'
#' Retrieves the runtime configuration interface for the Spark Context.
#'
#' @param sc A \code{spark_connection}.
#'
#' @export
spark_context_config <- function(sc) {
  sparkConfigAll <- spark_context(sc) %>%
    invoke("%>%", list("conf"), list("getAll"))
  sparkConfigNames <- lapply(sparkConfigAll, function(e) invoke(e, "_1")) %>%
    as.list()
  sparkConfig <- lapply(sparkConfigAll, function(e) invoke(e, "_2")) %>%
    as.list()
  names(sparkConfig) <- sparkConfigNames

  sparkConfig
}

#' Runtime configuration interface for Hive
#'
#' Retrieves the runtime configuration interface for Hive.
#'
#' @param sc A \code{spark_connection}.
#'
#' @export
hive_context_config <- function(sc) {
  if (spark_version(sc) < "2.0.0") {
    hive_context(sc) %>% invoke("getAllConfs")
  } else {
    hive_context(sc) %>% invoke("%>%", list("conf"), list("getAll"))
  }
}

#' Runtime configuration interface for the Spark Session
#'
#' Retrieves or sets runtime configuration entries for the Spark Session
#'
#' @param sc A \code{spark_connection}.
#' @param config The configuration entry name(s) (e.g., \code{"spark.sql.shuffle.partitions"}).
#'   Defaults to \code{NULL} to retrieve all configuration entries.
#' @param value The configuration value to be set. Defaults to \code{NULL} to retrieve
#'   configuration entries.
#'
#' @rdname spark_configuration
#' @family Spark runtime configuration
#'
#' @export
spark_session_config <- function(sc, config = TRUE, value = NULL) {
  if (is.null(value)) {
    sc %>%
      hive_context_config() %>%
      `[`(config)
  } else {
    spark_require_version(sc, "2.0.0")

    if (is.numeric(value)) {
      value <- as.integer(value)
    } else if (!is.logical(value) && !is.character(value)) {
      stop("Only logical, integer (long), and character values are allowed.")
    }
    sc %>%
      spark_session() %>%
      invoke("%>%", list("conf"), list("set", config, value)) %>%
      invisible()
  }
}

#' Retrieves or sets status of Spark AQE
#'
#' Retrieves or sets whether Spark adaptive query execution is enabled
#'
#' @param sc A \code{spark_connection}.
#' @param enable Whether to enable Spark adaptive query execution. Defaults to
#'   \code{NULL} to retrieve configuration entries.
#'
#' @family Spark runtime configuration
#'
#' @export
spark_adaptive_query_execution <- function(sc, enable = NULL) {
  if (is.null(enable)) {
    spark_session_config(sc, "spark.sql.adaptive.enabled")
  } else {
    spark_session_config(sc, "spark.sql.adaptive.enabled", enable)
  }
}

#' Retrieves or sets whether coalescing contiguous shuffle partitions is enabled
#'
#' Retrieves or sets whether coalescing contiguous shuffle partitions is enabled
#'
#' @param sc A \code{spark_connection}.
#' @param enable Whether to enable coalescing of contiguous shuffle partitions.
#'   Defaults to \code{NULL} to retrieve configuration entries.
#'
#' @family Spark runtime configuration
#'
#' @export
spark_coalesce_shuffle_partitions <- function(sc, enable = NULL) {
  if (spark_version(sc) < "3.0") {
    warning(
      "Coalescing coalesce shuffle partitions is only supported in Spark 3.0 ",
      "or above."
    )
  }
  if (is.null(enable)) {
    spark_session_config(sc, "spark.sql.adaptive.coalescePartitions.enabled")
  } else {
    if (enable) {
      spark_adaptive_query_execution(sc, TRUE)
    }
    spark_session_config(
      sc,
      "spark.sql.adaptive.coalescePartitions.enabled",
      enable
    )
  }
}

#' Retrieves or sets advisory size of the shuffle partition
#'
#' Retrieves or sets advisory size in bytes of the shuffle partition during adaptive optimization
#'
#' @param sc A \code{spark_connection}.
#' @param size Advisory size in bytes of the shuffle partition.
#'   Defaults to \code{NULL} to retrieve configuration entries.
#'
#' @family Spark runtime configuration
#'
#' @export
spark_advisory_shuffle_partition_size <- function(sc, size = NULL) {
  if (spark_version(sc) < "3.0") {
    warning(
      "Setting advisory size of shuffle partition during adaptive optimization",
      " is only supported in Spark 3.0 or above."
    )
  }
  # but proceed anyways even if we don't detect Spark version 3 or above
  if (is.null(size)) {
    spark_session_config(
      sc,
      "spark.sql.adaptive.advisoryPartitionSizeInBytes"
    )
  } else {
    spark_coalesce_shuffle_partitions(sc, TRUE)
    spark_session_config(
      sc,
      "spark.sql.adaptive.advisoryPartitionSizeInBytes",
      size
    )
  }
}

#' Retrieves or sets initial number of shuffle partitions before coalescing
#'
#' Retrieves or sets initial number of shuffle partitions before coalescing
#'
#' @param sc A \code{spark_connection}.
#' @param num_partitions Initial number of shuffle partitions before coalescing.
#'   Defaults to \code{NULL} to retrieve configuration entries.
#'
#' @family Spark runtime configuration
#'
#' @export
spark_coalesce_initial_num_partitions <- function(sc, num_partitions = NULL) {
  if (spark_version(sc) < "3.0") {
    warning(
      "Setting initial number of shuffle partitions before coalescing is only ",
      "supported in Spark 3.0 or above."
    )
  }
  # but proceed anyways even if we don't detect Spark version 3 or above
  if (is.null(num_partitions)) {
    spark_session_config(
      sc,
      "spark.sql.adaptive.coalescePartitions.initialPartitionNum"
    )
  } else {
    spark_coalesce_shuffle_partitions(sc, TRUE)
    spark_session_config(
      sc,
      "spark.sql.adaptive.coalescePartitions.initialPartitionNum",
      num_partitions
    )
  }
}

#' Retrieves or sets the minimum number of shuffle partitions after coalescing
#'
#' Retrieves or sets the minimum number of shuffle partitions after coalescing
#'
#' @param sc A \code{spark_connection}.
#' @param num_partitions Minimum number of shuffle partitions after coalescing.
#'   Defaults to \code{NULL} to retrieve configuration entries.
#'
#' @family Spark runtime configuration
#'
#' @export
spark_coalesce_min_num_partitions <- function(sc, num_partitions = NULL) {
  if (spark_version(sc) < "3.0") {
    warning(
      "Setting minimum number of shuffle partitions after coalescing is only ",
      "supported in Spark 3.0 or above."
    )
  }
  # but proceed anyways even if we don't detect Spark version 3 or above
  if (is.null(num_partitions)) {
    spark_session_config(
      sc,
      "spark.sql.adaptive.coalescePartitions.minPartitionNum"
    )
  } else {
    spark_coalesce_shuffle_partitions(sc, TRUE)
    spark_session_config(
      sc,
      "spark.sql.adaptive.coalescePartitions.minPartitionNum",
      num_partitions
    )
  }
}

#' Retrieves or sets the auto broadcast join threshold
#'
#' Configures the maximum size in bytes for a table that will be broadcast to all worker nodes
#' when performing a join. By setting this value to -1 broadcasting can be disabled. Note that
#' currently statistics are only supported for Hive Metastore tables where the command
#' `ANALYZE TABLE <tableName> COMPUTE STATISTICS noscan` has been run, and file-based data source
#' tables where the statistics are computed directly on the files of data.
#'
#' @param sc A \code{spark_connection}.
#' @param threshold Maximum size in bytes for a table that will be broadcast to all worker nodes
#'   when performing a join. Defaults to \code{NULL} to retrieve configuration entries.
#'
#' @family Spark runtime configuration
#'
#' @export
spark_auto_broadcast_join_threshold <- function(sc, threshold = NULL) {
  spark_session_config(
    sc,
    "spark.sql.autoBroadcastJoinThreshold",
    threshold
  )
}

spark_version_clean <- function(version) {
  gsub("\\.$", "", gsub("([0-9]+\\.?)[^0-9\\.](.*)", "\\1", version))
}

#' Get the Spark Version Associated with a Spark Connection
#'
#' Retrieve the version of Spark associated with a Spark connection.
#'
#' Suffixes for e.g. preview versions, or snapshotted versions,
#' are trimmed -- if you require the full Spark version, you can
#' retrieve it with \code{invoke(spark_context(sc), "version")}.
#'
#' @param sc A \code{spark_connection}.
#'
#' @return The Spark version as a \code{\link{numeric_version}}.
#'
#' @export
spark_version <- function(sc) {
  UseMethod("spark_version")
}

#' @export
spark_version.default <- function(sc) {
  # use cached value if available
  if (!is.null(sc$state$spark_version)) {
    return(sc$state$spark_version)
  }

  # get the version
  version <- invoke(spark_context(sc), "version")

  # Get rid of -preview and other suffix variations
  version <- spark_version_clean(version)

  # cache as numeric version
  sc$state$spark_version <- numeric_version(version)

  # return to caller
  sc$state$spark_version
}

spark_version_from_home_version <- function() {
  version <- Sys.getenv("SPARK_HOME_VERSION")
  if (nzchar(version)) version else NULL
}

#' Get the Spark Version Associated with a Spark Installation
#'
#' Retrieve the version of Spark associated with a Spark installation.
#'
#' @param spark_home The path to a Spark installation.
#' @param default The default version to be inferred, in case
#'   version lookup failed, e.g. no Spark installation was found
#'   at \code{spark_home}.
#'
#' @export
spark_version_from_home <- function(spark_home, default = NULL) {
  versionAttempts <- list(
    useDefault = function() {
      default
    },
    useEnvironmentVariable = function() {
      spark_version_from_home_version()
    },
    useReleaseFile = function() {
      versionedFile <- file.path(spark_home, "RELEASE")
      if (file.exists(versionedFile)) {
        releaseContents <- readLines(versionedFile)

        if (!is.null(releaseContents) && length(releaseContents) > 0) {
          gsub("Spark | built.*", "", releaseContents[[1]])
        }
      }
    },
    useAssemblies = function() {
      candidateVersions <- list(
        list(
          path = "lib",
          pattern = "spark-assembly-([0-9\\.]*)-hadoop.[0-9\\.]*\\.jar"
        ),
        list(
          path = "yarn",
          pattern = "spark-([0-9\\.]*)-preview-yarn-shuffle\\.jar"
        ),
        list(path = "yarn", pattern = "spark-([0-9\\.]*)-yarn-shuffle\\.jar"),
        list(
          path = "lib",
          pattern = "spark-([0-9\\.]*)-preview-yarn-shuffle\\.jar"
        ),
        list(path = "lib", pattern = "spark-([0-9\\.]*)-yarn-shuffle\\.jar"),
        list(
          path = "lib",
          pattern = "spark-assembly-([0-9\\.]*)-cdh[0-9\\.]*-hadoop.[0-9\\.]*\\.jar"
        )
      )

      candidateFiles <- lapply(candidateVersions, function(e) {
        c(
          e,
          list(
            files = list.files(
              file.path(spark_home, e$path),
              pattern = e$pattern,
              recursive = TRUE
            )
          )
        )
      })

      filteredCandidates <- Filter(
        function(f) length(f$files) > 0,
        candidateFiles
      )
      if (length(filteredCandidates) > 0) {
        valid <- filteredCandidates[[1]]
        e <- regexec(valid$pattern, valid$files[[1]])
        match <- regmatches(valid$files[[1]], e)
        if (length(match) > 0 && length(match[[1]]) > 1) {
          return(match[[1]][[2]])
        }
      }
    },
    useSparkSubmit = function() {
      version_output <- system2(
        file.path(spark_home, "bin", "spark-submit"),
        "--version",
        stderr = TRUE,
        stdout = TRUE
      )

      version_matches <- regmatches(
        version_output,
        regexec("   version (.*)$", version_output)
      )
      if (any(sapply(version_matches, length) > 0)) {
        version_row <- which(sapply(version_matches, length) > 0)
        return(version_matches[[version_row]][2])
      }
    }
  )

  for (versionAttempt in versionAttempts) {
    result <- versionAttempt()
    if (length(result) > 0) {
      return(spark_version_clean(result))
    }
  }

  stop(
    "Failed to detect version from SPARK_HOME or SPARK_HOME_VERSION. ",
    "Try passing the spark version explicitly."
  )
}

spark_version_latest <- function(version = NULL) {
  versions_full <- spark_available_versions(
    show_minor = TRUE,
    show_future = TRUE
  )
  versions <- sort(versions_full$spark, decreasing = TRUE)
  ret <- head(versions, 1)
  if (!is.null(version)) {
    match_versions <- versions[substr(versions, 1, nchar(version)) == version]
    if (length(match_versions) > 0) {
      ret <- head(match_versions, 1)
    }
  }
  ret
}

#' Find the SPARK_HOME directory for a version of Spark
#'
#' Find the SPARK_HOME directory for a given version of Spark that
#' was previously installed using \code{\link[=spark_install]{spark_install}}.
#'
#' @param version Version of Spark
#' @param hadoop_version Version of Hadoop
#'
#' @return Path to SPARK_HOME (or \code{NULL} if the specified version
#'   was not found).
#'
#' @keywords internal
#'
#' @export
spark_home_dir <- function(version = NULL, hadoop_version = NULL) {
  tryCatch(
    {
      installInfo <- spark_install_find(
        version = version,
        hadoop_version = hadoop_version
      )

      installInfo$sparkVersionDir
    },
    error = function(e) {
      NULL
    }
  )
}


#' Set the SPARK_HOME environment variable
#'
#' Set the \code{SPARK_HOME} environment variable. This slightly speeds up some
#' operations, including the connection time.
#'
#' @param path A string containing the path to the installation location of
#' Spark. If \code{NULL}, the path to the most latest Spark/Hadoop versions is
#' used.
#' @param ... Additional parameters not currently used.
#'
#' @return The function is mostly invoked for the side-effect of setting the
#' \code{SPARK_HOME} environment variable. It also returns \code{TRUE} if the
#' environment was successfully set, and \code{FALSE} otherwise.
#'
#' @examples
#' \dontrun{
#' # Not run due to side-effects
#' spark_home_set()
#' }
#' @export
spark_home_set <- function(path = NULL, ...) {
  verbose <- spark_config_value(list(), "sparklyr.verbose", is.null(path))
  if (is.null(path)) {
    path <- spark_install_find()$sparkVersionDir
  }
  if (verbose) {
    message("Setting SPARK_HOME environment variable to ", path)
  }
  Sys.setenv(SPARK_HOME = path)
}

# ------------ Init session level variables ----------
.gls_env <- new.env(parent = emptyenv())

.gls_env$extension_packages <- character()
.gls_env$spark_versions_json <- NULL
.gls_env$param_mapping_s_to_r <- NULL
.gls_env$param_mapping_r_to_s <- NULL
.gls_env$ml_class_mapping <- NULL
.gls_env$ml_package_mapping <- NULL
.gls_env$avail_package_cache <- NULL
.gls_env$do_spark <- NULL
.gls_env$last_error <- NULL

# ---------- Manage session level variables ----------
genv_get_last_error <- function() {
  .gls_env$last_error
}

genv_set_last_error <- function(x) {
  .gls_env$last_error <- x
  invisible()
}

genv_get_extension_packages <- function() {
  .gls_env$extension_packages
}

genv_set_extension_packages <- function(x) {
  .gls_env$extension_packages <- x
  invisible()
}

genv_get_spark_versions_json <- function() {
  .gls_env$spark_versions_json
}

genv_set_spark_versions_json <- function(x) {
  .gls_env$spark_versions_json <- x
  invisible()
}

genv_get_param_mapping_s_to_r <- function() {
  .gls_env$param_mapping_s_to_r
}

genv_set_param_mapping_s_to_r <- function(x) {
  .gls_env$param_mapping_s_to_r <- x
  invisible()
}

genv_get_param_mapping_r_to_s <- function() {
  .gls_env$param_mapping_r_to_s
}

genv_set_param_mapping_r_to_s <- function(x) {
  .gls_env$param_mapping_r_to_s <- x
  invisible()
}

genv_get_ml_class_mapping <- function() {
  .gls_env$ml_class_mapping
}

genv_set_ml_class_mapping <- function(x) {
  .gls_env$ml_class_mapping <- x
  invisible()
}

genv_get_ml_package_mapping <- function() {
  .gls_env$ml_package_mapping
}

genv_set_ml_package_mapping <- function(x) {
  .gls_env$ml_package_mapping <- x
  invisible()
}

genv_get_avail_package_cache <- function() {
  .gls_env$avail_package_cache
}

genv_set_avail_package_cache <- function(x) {
  .gls_env$avail_package_cache <- x
  invisible()
}

genv_get_do_spark <- function(element = NULL) {
  if (!is.null(element)) {
    .gls_env$do_spark[[element]]
  } else {
    .gls_env$do_spark
  }
}

genv_set_do_spark <- function(x) {
  .gls_env$do_spark <- x
  invisible()
}

genv_clear_do_spark_options <- function() {
  remove(list = ls(.gls_env$do_spark$options), pos = .gls_env$do_spark$options)
}

genv_set_do_spark_options <- function(optnames, opts) {
  for (i in seq(along = opts)) {
    assign(optnames[i], opts[[i]], pos = .gls_env$do_spark$options)
  }
}
