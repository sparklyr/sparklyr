#' Connect to Spark for Dplyr.
#'
#' @import dplyr
#' @export
src_spark <- function(master = "local",
                      appName = "dplyrspark") {
  setup_local()
  con <- dbConnect(DBISpark(master, appName))
  src_sql("spark", con)
}

#' @export
src_desc.src_spark <- function(con) {
  cat(paste("spark connection", paste("master", con$master, sep = "="), paste("app", con$appName, sep = "=")))
}

#' @export
db_explain.src_spark <- function(con) {
  ""
}

#' @export
tbl.src_spark <- function(src, from, ...) {
  make_tbl(c("spark", "sql", "lazy"), src = src, ops = dplyr:::op_base_remote(src, from))

  # TODO(dplyr): Uncomment once subclassing tables is supported
  # tbl_sql("spark", src = src, from = from, ...)
}

#' @export
db_data_type.src_spark <- function(...) {
}

#' @export
sql_begin.src_spark <- function(...) {
}

#' @export
sql_commit.src_spark <- function(...) {
}

#' @export
sql_rollback.src_spark <- function(...) {
}

#' @export
sql_create_table.src_spark <- function(...) {
}

#' @export
sql_insert_into.src_spark <- function(...) {
}

#' @export
sql_drop_table.src_spark <- function(con, name) {
  dbRemoveTable(con, name)
}

#' @export
copy_to.src_spark <- function(con, df, name) {
  dbWriteTable(con$con, name, df)
}

#' @export
sql_create_index.src_spark <- function(...) {
}

#' @export
sql_analyze.src_spark <- function(...) {
}

#' @export
print.src_spark <- function(db = db, n = 5) {
  cat(paste("src:  ", src_desc(db), sep = ""))
  cat("log:")

  log <- file(db$con@con$outputFile)
  lines <- readLines(log)
  close(log)

  lines <- tail(lines, n = n)

  cat(paste(lines, collapse = "\n"))
}
