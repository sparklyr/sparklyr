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
  spark_api_copy_data(con, name, df)
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
  query <- build_sql("SELECT * FROM ",
                     sql,
                     " LIMIT 1",
                     con = con)

  sqlResult <- spark_api_sql(con, as.character(query))
  df <- spark_api_data_frame(con, sqlResult)
  names(df)
}

#' @export
sql_select.SparkConnection <- function(con, select, from, where = NULL,
                                       group_by = NULL, having = NULL,
                                       order_by = NULL, limit = NULL,
                                       offset = NULL, ...) {
  dplyr:::sql_select.default(con, select, from, where = where,
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
  spark_api_sql(con, sql)
}

#' @export
sql_escape_ident.SparkConnection <- function(con, x) {
  sql_quote(x, '`')
}

