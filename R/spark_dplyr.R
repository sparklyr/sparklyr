#' Connect to Spark for Dplyr.
#'
#' @import dplyr
#' @export
src_spark <- function(master = "local") {
  con <- start_shell()

  attr(con, "class") <- "SparkConnection"

  src_sql("spark", con)
}

db_has_table.SparkConnection <- function(con, table, ...) {
  NA
}

db_list_tables.SparkConnection <- function(con) {
  # TODO: Run "show tables" through sqlContext
  character(0)
}

db_data_type.SparkConnection <- function(con, table, ...) {
  sapply(names(table), function(x) { "String" })
}

#' @export
src_desc.src_spark <- function(con) {
  info <- dbGetInfo(con)

  "spark connection"
}
