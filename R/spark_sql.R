# Spark SQL functionalities shared among dplyr_spark.R, dplyr_spark_connection.R,
# and dbi_spark_connection.R

#' @include connection_spark.R
#' @include sql_utils.R
#' @include tbl_spark.R
NULL

#' @importFrom dbplyr build_sql
#' @importFrom DBI dbExecute
spark_db_analyze <- function(con, table, ...) {
  if (spark_version(con) < "2.0.0") {
    return(NULL)
  }

  info <- dbGetQuery(con, build_sql("SHOW TABLES LIKE", table, con = con))
  if (nrow(info) > 0 && identical(info$isTemporary, FALSE)) {
    dbExecute(con, build_sql("ANALYZE TABLE", table, "COMPUTE STATISTICS", con = con))
  }
}

spark_db_desc <- function(x) {
  sc <- spark_connection(x)

  paste(
    "spark connection",
    paste("master", sc$master, sep = "="),
    paste("app", sc$app_name, sep = "="),
    paste("local", spark_connection_is_local(sc), sep = "=")
  )
}

#' @importFrom dbplyr build_sql
spark_sql_query_explain <- function(con, sql, ...) {
  build_sql("EXPLAIN ", sql, con = con)
}

#' @importFrom dbplyr sql
spark_db_query_fields_sql <- function(con, query) {
  sql_select_impl <- (
    if (utils::packageVersion("dbplyr") < "2") {
      dplyr::sql_select
    } else {
      dbplyr::sql_query_select
    })
  sql_subquery_impl <- (
    if (utils::packageVersion("dbplyr") < "2") {
      dplyr::sql_subquery
    } else {
      dbplyr::sql_query_wrap
    })

  sql_select_impl(
    con,
    sql("*"),
    sql_subquery_impl(con, query),
    where = sql("0 = 1")
  )
}

spark_db_query_fields <- function(con, query) {
  fields_sql <- spark_db_query_fields_sql(con, query)

  hive_context(con) %>%
    invoke("sql", as.character(fields_sql)) %>%
    invoke("schema") %>%
    invoke("fieldNames") %>%
    unlist() %>%
    as.character()
}

#' @importFrom dbplyr sql
spark_sql_query_fields <- function(con, query, ...) {
  # NOTE: this is a workaround for limitation in some version(s) of Arrow
  columns <- spark_db_query_fields(con, query)
  if (length(columns) == 0) {
    # Special case: if the dataframe has no column at all, then the workaround
    # below cannot be applied and simply `SELECT * FROM (<query>) WHERE (0 = 1)`
    # will be fine for both Arrow and non-Arrow use cases
    spark_db_query_fields_sql(con, query)
  } else {
    # If there is at least 1 column and 0 row, then some version(s) of Arrow
    # will not collect the data into a R dataframe with an equivalent schema,
    # hence the workaround below
    columns_sql_lst <- columns %>%
      lapply(function(x) sprintf("0L AS %s", quote_sql_name(x)))
    columns_sql <- sprintf(
      "SELECT %s", do.call(paste, append(columns_sql_lst, list(sep = ", ")))
    )

    columns_sql %>% sql()
  }
}

#' @importFrom dbplyr as.sql
#' @importFrom dbplyr build_sql
#' @importFrom dbplyr sql
spark_sql_query_save <- function(con, sql, name, temporary = TRUE, ...) {
  build_sql(
    "CREATE ", if (temporary) sql("TEMPORARY "), "VIEW \n",
    as.sql(name), " AS ", sql,
    con = con
  )
}

#' @importFrom dbplyr build_sql
#' @importFrom dbplyr sql
spark_sql_set_op <- function(con, x, y, method) {
  if (spark_version(con) < "2.0.0") {
    # Spark 1.6 does not allow parentheses
    build_sql(
      x,
      "\n", sql(method), "\n",
      y
    )
  } else {
    NULL
  }
}
