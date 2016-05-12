#' Connect to Spark for Dplyr.
#'
#' @import dplyr
#' @export
#' @param scon Spark connection provided by spark_connection
src_spark <- function(scon) {
  con <- dbConnect(DBISpark(scon))
  src_sql("spark", con)
}

#' @export
src_desc.src_spark <- function(db) {
  paste("spark connection", paste("master", db$con@scon$master, sep = "="), paste("app", db$con@scon$appName, sep = "="))
}

#' @export
db_explain.src_spark <- function(con) {
  ""
}

#' @export
tbl.src_spark <- function(src, from, ...) {
  tbl_sql("spark", src = src, from = from, ...)
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
db_data_type.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
sql_begin.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
sql_commit.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
sql_rollback.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
sql_create_table.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
sql_insert_into.src_spark <- function(...) {
}

#' Removes a Spark table
#' @export
#' @param con Connection to dplyr source
#' @param name Name of the table to remove
sql_drop_table.src_spark <- function(con, name) {
  dbRemoveTable(con, name)
}

#' Copies the source data frame into a Spark table
#' @export
#' @param con Connection to dplyr source
#' @param df Data frame to copy from
#' @param name Name of the destination table
copy_to.src_spark <- function(con, df, name) {
  dbWriteTable(con$con, name, df)
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
sql_create_index.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
#' @param ... Additional parameters
sql_analyze.src_spark <- function(...) {
}

#' Prints information associated to the dplyr source
#' @export
#' @param x Reference to dplyr source
#' @param ... Additional parameters
print.src_spark <- function(x, ...) {
  cat(src_desc(db))
  cat("\n\n")

  spark_log(x$con@scon)
}

