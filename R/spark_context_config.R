#' Runtime configuration interface for Spark.
#'
#' Retrieves the runtime configuration interface for Spark.
#'
#' @param sc A \code{spark_connection}.
#'
#' @export
spark_context_config <- function(sc)
{
  sparkConfigAll <- spark_context(sc) %>% invoke("conf") %>% invoke("getAll")
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
hive_context_config <- function(sc)
{
  if (spark_version(sc) < "2.0.0")
    hive_context(sc) %>% invoke("getAllConfs")
  else
    hive_context(sc) %>% invoke("conf") %>% invoke("getAll")
}
