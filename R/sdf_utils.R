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
                           "sparklyr.DFUtils",
                           "zipDataFrames",
                           spark_context(sc),
                           df1, df2)

  Reduce(zip_sdf, lapply(dots, spark_dataframe)) %>%
    sdf_register()
}

#' Debug Info for Spark DataFrame
#'
#' Prints plan of execution to generate \code{x}. This plan will, among other things, show the
#' number of partitions in parenthesis at the far left and indicate stages using indentation.
#'
#' @param x An \R object wrapping, or containing, a Spark DataFrame.
#' @param print Print debug information?
#'
#' @export
sdf_debug_string <- function(x, print = TRUE) {
  debug_string <- x %>%
    spark_dataframe() %>%
    invoke("%>%", list("rdd"), list("toDebugString"))

  if (print)
    cat(debug_string)

  debug_string %>%
    strsplit("\n", fixed=TRUE) %>%
    unlist() %>%
    invisible()
}
