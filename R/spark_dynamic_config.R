#' Configurations for open spark session
#' 
#' These functions will get/set spark configurations on a spark session that is already open.
#' 
#' @param sc A \code{spark_connection}.
#' @param config Character. The name of a spark configuration (e.g., "spark.sql.shuffle.partitions").
#' @param value Logical, Integer, or Character configuration value to set.
#' @export
spark_get_config <- function(sc, config) {
  if (spark_version(sc) < "2.0.0")
    stop("spark_get_config() requires Spark 2.0.0+")
  
  sc %>%
    spark_session() %>%
    invoke("conf") %>% 
    invoke("get", config)
}

#' @rdname spark_get_conf
#' @export
spark_set_config <- function(sc, config, value) {
  if (spark_version(sc) < "2.0.0")
    stop("spark_set_config() requires Spark 2.0.0+")
  
  if (is.numeric(value))
    value <- as.integer(value)
  else if (!is.logical(value) && !is.character(value))
    stop("Only logical, integer (long), and character values are allowed.")
  sc %>%
    spark_session() %>%
    invoke("conf") %>% 
    invoke("set", config, value) %>%
    invisible()
}

