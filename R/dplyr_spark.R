#' @import dplyr

#' @export
spark_connection.tbl_spark <- function(x, ...) {
  spark_connection(x$src)
}

#' @export
spark_connection.src_spark <- function(x, ...) {
  x$con
}

#' @export
src_desc.src_spark <- function(x) {
  sc <- spark_connection(x)
  paste("spark connection",
        paste("master", sc$master, sep = "="),
        paste("app", sc$app_name, sep = "="),
        paste("local", spark_connection_is_local(sc), sep = "="))
}

#' @export
db_explain.src_spark <- function(con, sql, ...) {
  ""
}

#' @export
tbl_vars.spark_jobj <- function(x) {
  as.character(invoke(x, "columns"))
}

#' @export
tbl.src_spark <- function(src, from, ...) {
  tbl_sql("spark", src = src, from = from, ...)
}

#' @export
tbl.spark_connection <- function(src, from, ...) {
  src <- src_sql("spark", src)
  tbl_sql("spark", src = src, from = from, ...)
}

#' @export
src_tbls.spark_connection <- function(x, ...) {
  sql <- hive_context(x)
  tbls <- invoke(sql, "sql", "SHOW TABLES")
  tableNames <- sdf_read_column(tbls, "tableName")

  filtered <- grep("^sparklyr_tmp_", tableNames, invert = TRUE, value = TRUE)
  sort(filtered)
}

#' @export
db_data_type.src_spark <- function(...) {
}


#' Copy an R Data Frame to Spark
#'
#' Copy an R \code{data.frame} to Spark, and return a reference to the
#' generated Spark DataFrame as a \code{tbl_spark}. The returned object will
#' act as a \code{dplyr}-compatible interface to the underlying Spark table.
#'
#' @param dest A \code{spark_connection}.
#' @param df An \R \code{data.frame}.
#' @param name The name to assign to the copied table in Spark.
#' @param memory Boolean; should the table be cached into memory?
#' @param repartition The number of partitions to use when distributing the
#'   table across the Spark cluster. The default (0) can be used to avoid
#'   partitioning.
#' @param overwrite Boolean; overwrite a pre-existing table with the name \code{name}
#'   if one already exists?
#' @param ... Optional arguments; currently unused.
#'
#' @return A \code{tbl_spark}, representing a \code{dplyr}-compatible interface
#'   to a Spark DataFrame.
#'
#' @export
copy_to.spark_connection <- function(dest,
                                     df,
                                     name = deparse(substitute(df)),
                                     memory = TRUE,
                                     repartition = 0L,
                                     overwrite = FALSE,
                                     ...)
{
  if (!is.data.frame(df)) {
    stop("copy_to expects a local data frame")
  }

  sc <- dest

  if (overwrite)
    spark_remove_table_if_exists(sc, name)

  if (name %in% src_tbls(sc))
    stop("table ", name, " already exists (pass overwrite = TRUE to overwrite)")

  dots <- list(...)
  serializer <- dots$serializer
  spark_data_copy(sc, df, name = name, repartition = repartition, serializer = serializer)

  if (memory)
    tbl_cache(sc, name)

  on_connection_updated(sc, name)

  tbl(sc, name)
}

#' @export
copy_to.src_spark <- function(dest, df, name, ...) {
  copy_to(spark_connection(dest), df, name, ...)
}

tbl_cache_sdf <- function(sc, name, force) {
  tbl <- tbl(sc, name)
  sdf <- spark_dataframe(tbl)

  invoke(sdf, "cache")
  if (force)
    invoke(sdf, "count")
}

tbl_cache_sql <- function(sc, name, force) {
  dbGetQuery(sc, paste("CACHE TABLE", dplyr::escape(ident(name), con = sc)))

  if (force)
    dbGetQuery(sc, paste("SELECT count(*) FROM", dplyr::escape(ident(name), con = sc)))
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
    tbl <- tbl(sc, name)
    sdf <- spark_dataframe(tbl)

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
  tbl <- tbl(sc, name)
  sdf <- spark_dataframe(tbl)
  invoke(sdf, "unpersist")
  invisible(NULL)
}

#' @export
print.src_spark <- function(x, ...) {
  cat(src_desc(x))
  cat("\n\n")

  spark_log(spark_connection(x))
}

#' @export
db_save_query.spark_connection <- function(con, sql, name, temporary = TRUE, ...)
{
  df <- spark_dataframe(con, sql)
  invoke(df, "registerTempTable", name)
}
