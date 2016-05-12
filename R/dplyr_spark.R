#' Connect to Spark for Dplyr.
#'
#' @import dplyr
#' @export
src_spark <- function(sc) {
  con <- dbConnect(DBISpark(sc))
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
db_data_type.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
sql_begin.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
sql_commit.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
sql_rollback.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
sql_create_table.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
sql_insert_into.src_spark <- function(...) {
}

#' Removes a Spark table
#' @export
sql_drop_table.src_spark <- function(con, name) {
  dbRemoveTable(con, name)
}

#' Copies the source data frame into a Spark table
#' @export
copy_to.src_spark <- function(con, df, name) {
  dbWriteTable(con$con, name, df)
}

#' This operation is currently not supported in Spark
#' @export
sql_create_index.src_spark <- function(...) {
}

#' This operation is currently not supported in Spark
#' @export
sql_analyze.src_spark <- function(...) {
}

#' @export
print.src_spark <- function(db = db, n = 5) {
  cat(src_desc(db))
  cat("\n\n")

  spark_log(db$con@scon)
}

