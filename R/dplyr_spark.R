#' @import dplyr

spark_dbi <- function(x, ...) {
  UseMethod("spark_dbi", x)
}

spark_dbi.src_spark <- function(x) {
  x$con
}

spark_dbi.spark_connection <- function(x) {
  dbConnect(DBISpark(x))
}

spark_api <- function(x) {
  spark_dbi(x)@api
}


#' @export
sparkapi_connection.tbl_spark <- function(x, ...) {
  sparkapi_connection(x$src)
}

#' @export
sparkapi_connection.src_spark <- function(x, ...) {
  spark_dbi(x)@scon
}


#' @export
src_desc.src_spark <- function(x) {
  scon <- src_context(x)
  paste("spark connection",
        paste("master", spark_connection_master(scon), sep = "="),
        paste("app", spark_connection_app_name(scon), sep = "="),
        paste("local", spark_connection_is_local(scon), sep = "="))
}

src_context <- function(db) {
  db$con@scon
}

#' @export
db_explain.src_spark <- function(con, sql, ...) {
  ""
}

#' @export
tbl_vars.sparkapi_jobj <- function(x) {
  as.character(spark_invoke(x, "columns"))
}

#' @export
tbl.src_spark <- function(src, from, ...) {
  tbl_sql("spark", src = src, from = from, ...)
}

#' @export
tbl.spark_connection <- function(src, from, ...) {
  src <- src_sql("spark", dbConnect(DBISpark(src)))
  tbl_sql("spark", src = src, from = from, ...)
}

#' @export
src_tbls.spark_connection <- function(x, ...) {
  src <- src_sql("spark", dbConnect(DBISpark(x)))
  sort(src_tbls(src, ...))
}

#' @export
db_data_type.src_spark <- function(...) {
}


#' Copy a local R dataframe to Spark and provide a data source compatible with dplyr
#'
#' Copy a local R dataframe to Spark and provide a data source compatible with dplyr
#'
#' @param dest A Spark connection
#' @param name Name of the destination table
#' @param df Local data frame to copy
#' @param ... Unused
#' @param memory Cache table into memory for improved performance
#' @param repartition Total of partitions used to distribute table or 0 (default) to avoid partitioning
#' @param overwrite When TRUE, overwrites table with existing name
#' @param local_file When TRUE, uses a local file to copy the data frame, this is only available in local installs.
#'
#' @name copy_to
#'
#' @export
copy_to.spark_connection <- function(dest, df, name = deparse(substitute(df)),
                                     memory = TRUE, repartition = 0, overwrite = FALSE, local_file = NULL, ...) {
  sc <- dest
  dest <- src_sql("spark", dbConnect(DBISpark(sc)))

  if (overwrite)
    spark_remove_table_if_exists(dest, name)
  if (name %in% src_tbls(sc))
    stop("table ", name, " already exists (pass overwrite = TRUE to overwrite)")

  dbWriteTable(dest$con, name, df, TRUE, repartition, local_file)

  if (memory) {
    tbl_cache(sc, name)
  }

  on_connection_updated(sc, name)

  tbl(dest, name)
}

#' Loads a table into memory
#' @export
#' @param sc Connection to dplyr source
#' @param name Name of the destination table
#' @param force Forces data to be loaded in memory by executing a count(*) over the table
tbl_cache <- function(sc, name, force = TRUE) {
  dbiCon <- dbConnect(DBISpark(sc))

  dbGetQuery(dbiCon, paste("CACHE TABLE", dplyr::escape(ident(name), con = dbiCon)))

  if (force) {
    dbGetQuery(dbiCon, paste("SELECT count(*) FROM", dplyr::escape(ident(name), con = dbiCon)))
  }
}

#' Unloads table from memory
#' @export
#' @param sc Connection to dplyr source
#' @param name Name of the destination table
tbl_uncache <- function(sc, name) {
  dbiCon <- dbConnect(DBISpark(sc))
  dbGetQuery(dbiCon, paste("UNCACHE TABLE", dplyr::escape(ident(name), con = dbiCon)))
}

#' @export
print.src_spark <- function(x, ...) {
  cat(src_desc(x))
  cat("\n\n")

  spark_log(x$con@scon)
}

#' @export
db_save_query.DBISparkConnection <- function (con, sql, name, temporary = TRUE, ...) 
{
  df <- sparkapi_dataframe(con@scon, sql)
  spark_invoke(df, "registerTempTable", name)
}