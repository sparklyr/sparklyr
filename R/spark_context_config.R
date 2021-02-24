#' Runtime configuration interface for the Spark Context.
#'
#' Retrieves the runtime configuration interface for the Spark Context.
#'
#' @param sc A \code{spark_connection}.
#'
#' @export
spark_context_config <- function(sc) {
  sparkConfigAll <- spark_context(sc) %>% invoke("%>%", list("conf"), list("getAll"))
  sparkConfigNames <- lapply(sparkConfigAll, function(e) invoke(e, "_1")) %>% as.list()
  sparkConfig <- lapply(sparkConfigAll, function(e) invoke(e, "_2")) %>% as.list()
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
  }
  else {
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
    spark_session_config(sc, "spark.sql.adaptive.coalescePartitions.enabled", enable)
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
      sc, "spark.sql.adaptive.advisoryPartitionSizeInBytes"
    )
  } else {
    spark_coalesce_shuffle_partitions(sc, TRUE)
    spark_session_config(
      sc, "spark.sql.adaptive.advisoryPartitionSizeInBytes", size
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
      sc, "spark.sql.adaptive.coalescePartitions.initialPartitionNum"
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
      sc, "spark.sql.adaptive.coalescePartitions.minPartitionNum"
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
    sc, "spark.sql.autoBroadcastJoinThreshold", threshold
  )
}
