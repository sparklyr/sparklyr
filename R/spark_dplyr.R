#' Connect to Spark for Dplyr.
#'
#' @import dplyr
#' @export
src_spark <- function(master = "local",
                      appName = "splyr") {
  con <- start_shell()

  con$sc <- spark_api_create_context(con, master, appName)
  if (identical(con$sc, NULL)) {
    stop("Failed to create Spark context")
  }

  con$sql <- spark_api_create_sql_context(con)
  if (identical(con$sc, NULL)) {
    stop("Failed to create SQL context")
  }

  attr(con, "class") <- "SparkConnection"

  src_sql("spark", con)
}

#' @export
db_has_table.SparkConnection <- function(con, table, ...) {
  NA
}

#' @export
db_list_tables.SparkConnection <- function(con) {
  sqlResult <- spark_api_sql(con, "SHOW TABLES")
  spark_api_data_frame(con, sqlResult)
}

#' @export
db_data_type.SparkConnection <- function(con, table, ...) {
  sapply(names(table), function(x) { "String" })
}

#' @export
src_desc.src_spark <- function(con) {
  "spark connection"
}
