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
  sapply(names(table), function(x) { "STRING" })
}

#' @export
db_begin.SparkConnection <- function(con) {
}

#' @export
db_rollback.SparkConnection <- function(con) {
}

#' @export
db_create_table.SparkConnection <- function(con, name, types, temporary = temporary) {
}

#' @export
db_insert_into.SparkConnection <- function(con, name, df) {
  tempfile <- tempfile(fileext = ".csv")
  write.csv(df, tempfile)
  spark_read_csv(con, tempfile)
}

#' @export
db_create_indexes.SparkConnection <- function(con, name, indexes) {
}

#' @export
db_analyze.SparkConnection <- function(con, name) {
}

#' @export
db_commit.SparkConnection <- function(con) {
}

#' @export
db_query_fields.SparkConnection <- function(con, sql) {
  query <- build_sql("SELECT * FROM ", sql, " LIMIT 1",  con = con)
  sqlResult <- spark_api_sql(con, as.character(query))
  df <- spark_api_data_frame(con, sqlResult)
  names(df)
}

#' @export
src_translate_env.src_spark <- function(x) {
  nyi <- function(...) stop("Currently not supported")

  dplyr::sql_variant(
    dplyr::sql_translator(
      .parent = dplyr::base_scalar,

      # Casting
      as.logical = dplyr::sql_prefix("string"),
      as.numeric = dplyr::sql_prefix("string"),
      as.double = dplyr::sql_prefix("string"),
      as.integer = dplyr::sql_prefix("string"),
      as.character = dplyr::sql_prefix("string"),

      # Date/time
      Sys.date = nyi,
      Sys.time = nyi,

      # Regular expressions
      grepl = nyi,
      gsub = nyi,

      # stringr equivalents
      str_detect = nyi,
      str_extract = nyi,
      str_replace = nyi
    ),
    dplyr::sql_translator(
      .parent = dplyr::base_agg,
      n = nyi,
      "%||%" = nyi,
      sd =nyi
    ),
    dplyr::sql_translator(
      .parent = dplyr::base_win,
      mean  = nyi,
      sum   = nyi,
      min   = nyi,
      max   = nyi,
      n     = nyi,
      cummean = nyi,
      cumsum  = nyi,
      cummin  = nyi,
      cummax  = nyi
    )
  )
}

#' @export
src_desc.src_spark <- function(con) {
  "spark connection"
}

#' @export
db_explain.src_spark <- function(con) {
  ""
}

#' @export
tbl.src_spark <- function(src, from, ...) {
  tbl_sql("spark", src = src, from = from, ...)
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
sql_drop_table.src_spark <- function(name) {
  spark_api_sql(con, paste("DROP TABLE '", name, "'", sep = ""))
}

#' @export
sql_create_index.src_spark <- function(...) {
}

#' @export
sql_analyze.src_spark <- function(...) {
}

#' @export
sql_select.SparkConnection <- function(con, select, from, where = NULL,
                                       group_by = NULL, having = NULL,
                                       order_by = NULL, limit = NULL,
                                       offset = NULL, ...) {
  dplyr:::sql_select.DBIConnection(con, select, from, where = where,
                                   group_by = group_by, having = having, order_by = order_by,
                                   limit = limit, offset = offset, ...)
}

#' @export
sql_subquery.SparkConnection <- function(con, sql, name =  dplyr::unique_name(), ...) {
  if (dplyr::is.ident(sql)) return(sql)

  dplyr::build_sql("(", sql, ") AS ", dplyr::ident(name), con = con)
}

#' @export
query.SparkConnection <- function(con, sql, .vars) {
}

#' @export
dim.tbl_spark <- function(x) {
  c(NA, 0)
}

#' @export
head.tbl_spark <- function(x, n) {
  list()
}
