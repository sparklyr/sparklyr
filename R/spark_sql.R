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

  # Disabling table analysis if the connection is using Delta catalog
  # It does this because SHOW TABLES does not correctly return isTemporary
  # TODO: Find a better way to determine if the table is Temporary when using
  # Delta on Spark 3.3 and above
  if("org.apache.spark.sql.delta.catalog.DeltaCatalog" %in%
     con$config$spark.sql.catalog.spark_catalog) {
    return(NULL)
  }

  table_q <- as.character(escape(table, con = con))
  table_name <- substr(table_q, 2, nchar(table_q) - 1)
  info <- dbGetQuery(con, build_sql("SHOW TABLES LIKE ", table_name, con = con))
  if (nrow(info) > 0 && identical(info$isTemporary, FALSE)) {
    dbExecute(con, build_sql("ANALYZE TABLE ", table, " COMPUTE STATISTICS", con = con))
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
  dbplyr::sql_query_select(
    con,
    sql("*"),
    dbplyr::sql_query_wrap(con, query),
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

#' @importFrom dbplyr build_sql
#' @importFrom dbplyr sql
spark_sql_query_save <- function(con, sql, name, temporary = TRUE, ...) {
  if (packageVersion("dbplyr") <= "2.3.4") {
    name <- dbplyr::as.sql(name)
  }
  build_sql(
    "CREATE OR REPLACE ", if (temporary) sql("TEMPORARY "), "VIEW \n",
    name, " AS ", sql,
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
