#' Connect to Spark for Dplyr.
#'
#' @import dplyr
#' @export
src_spark <- function(master = "local",
                      appName = "splyr") {
  con <- start_shell()

  con$sc <- spark_api_create_context(con, master, appName)
  con$sql <- spark_api_create_sql_context(con)

  attr(con, "class") <- "SparkConnection"

  src_sql("spark", con)
}

#' @export
db_has_table.SparkConnection <- function(con, table, ...) {
  NA
}

#' @export
db_list_tables.SparkConnection <- function(con) {
  spark_api_sql(con, "SHOW TABLES")
}

#' @export
db_data_type.SparkConnection <- function(con, table, ...) {
  sapply(names(table), function(x) { "String" })
}

#' @export
src_desc.src_spark <- function(con) {
  "spark connection"
}
