tbl_quote_name <- function(name) {
  x <- gsub("'", "''", name, fixed = TRUE)
  paste("'", name, "'", sep = "")
}

tbl_cache_sdf <- function(sc, name, force) {
  tbl <- tbl(sc, name)
  sdf <- spark_dataframe(tbl)

  invoke(sdf, "cache")
  if (force)
    invoke(sdf, "count")
}

tbl_cache_sql <- function(sc, name, force) {
  sql <- paste("CACHE TABLE", tbl_quote_name(name))
  invoke(hive_context(sc), "sql", sql)

  if (force) {
    sql <- paste("SELECT count(*) FROM ", tbl_quote_name(name))
    sdf <- invoke(hive_context(sc), "sql", sql)
    sdf_collect(sdf)
  }
}

#' Cache a Spark Table
#'
#' Force a Spark table with name \code{name} to be loaded into memory.
#' Operations on cached tables should normally (although not always)
#' be more performant than the same operation performed on an uncached
#' table.
#'
#' @param sc A \code{spark_connection}.
#' @param name The table name.
#' @param force Force the data to be loaded into memory? This is accomplished
#'   by calling the \code{count} API on the associated Spark DataFrame.
#'
#' @export
tbl_cache <- function(sc, name, force = TRUE) {
  countColumns <- function(sc, name) {
    sql <- paste("SELECT * FROM ", tbl_quote_name(name))
    sdf <- invoke(hive_context(sc), "sql", sql)

    length(invoke(sdf, "columns"))
  }

  # We preffer to cache tables using SQL syntax since this would track the
  # table names in logs and ui with a friendly name, say "In-memory table df".
  # Using tbl_cache_sdf is supported for high-number of columns; however, it
  # displays a non-friendly name that we try to avoid.

  if (spark_version(sc) < "2.0.0" && countColumns(sc, name) >= 1000)
    tbl_cache_sdf(sc, name, force)
  else
    tbl_cache_sql(sc, name, force)

  invisible(NULL)
}

#' Uncache a Spark Table
#'
#' Force a Spark table with name \code{name} to be unloaded from memory.
#'
#' @param sc A \code{spark_connection}.
#' @param name The table name.
#'
#' @export
tbl_uncache <- function(sc, name) {
  sql <- paste("UNCACHE TABLE", tbl_quote_name(name))
  invoke(hive_context(sc), "sql", sql)

  invisible(NULL)
}
