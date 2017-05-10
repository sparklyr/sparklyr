#' Fast cbind for Spark DataFrames
#'
#' This is a version of `sdf_bind_cols` that works by zipping
#' RDDs. From the API docs: "Assumes that the two RDDs have the
#' *same number of partitions* and the *same number of elements
#' in each partition* (e.g. one was made through a map on the
#' other)."
#'
#' @keywords internal
#'
#' @param ... Spark DataFrames to cbind
sdf_fast_bind_cols <- function(...) {

  dots <- list(...)
  n <- length(dots)
  self <- dots[[1]]
  if (n == 1)
    return(self)

  sc <- self %>%
    spark_dataframe() %>%
    spark_connection()

  zip_sdf <- function(df1, df2) invoke_static(sc,
                           "sparklyr.Utils",
                           "zipDataFrames",
                           spark_context(sc),
                           df1, df2)

  Reduce(zip_sdf, lapply(dots, spark_dataframe)) %>%
    sdf_register()
}
