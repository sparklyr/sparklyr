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
