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


#' Copy a local R data frame to Spark
#'
#' @param dest A Spark connection
#' @param df Local data frame to copy
#' @param name Name of the destination table
#' @param memory Cache table into memory
#' @param repartition Partitions used to distribute table or 0 (default) to avoid partitioning
#' @param overwrite When TRUE, overwrites table with existing name
#' @param ... Unused
#'
#' @return dplyr compatible reference to table
#'
#' @name copy_to
#'
#' @family dplyr
#'
#' @export
copy_to.spark_connection <- function(dest, df, name = deparse(substitute(df)),
                                     memory = TRUE, repartition = 0, overwrite = FALSE, ...) {
  sc <- dest
  dest <- src_sql("spark", sc)
  args <- list(...)

  if (overwrite)
    spark_remove_table_if_exists(sc, name)

  if (name %in% src_tbls(sc))
    stop("table ", name, " already exists (pass overwrite = TRUE to overwrite)")

  dbWriteTable(sc, name, df, TRUE, repartition, args$serializer)

  if (memory) {
    tbl_cache(sc, name)
  }

  on_connection_updated(sc, name)

  tbl(dest, name)
}

#' Load a table into memory
#'
#' @param sc Spark connection
#' @param name Name of the destination table
#' @param force Forces data to be loaded in memory by executing a count(*) over the table
#'
#' @family dplyr
#'
#' @export
tbl_cache <- function(sc, name, force = TRUE) {
  dbGetQuery(sc, paste("CACHE TABLE", dplyr::escape(ident(name), con = sc)))

  if (force) {
    dbGetQuery(sc, paste("SELECT count(*) FROM", dplyr::escape(ident(name), con = sc)))
  }

  invisible(NULL)
}

#' Unload table from memory
#'
#' @param sc Spark connection
#' @param name Name of the destination table
#'
#' @family dplyr
#'
#' @export
tbl_uncache <- function(sc, name) {
  dbGetQuery(sc, paste("UNCACHE TABLE", dplyr::escape(ident(name), con = sc)))
  invisible(NULL)
}

#' @export
print.src_spark <- function(x, ...) {
  cat(src_desc(x))
  cat("\n\n")

  spark_log(spark_connection(x))
}

#' @export
db_save_query.spark_connection <- function (con, sql, name, temporary = TRUE, ...)
{
  df <- spark_dataframe(con, sql)
  invoke(df, "registerTempTable", name)
}
