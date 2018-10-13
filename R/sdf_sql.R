
df_from_sql <- function(sc, sql, arrow = TRUE) {
  sdf <- invoke(hive_context(sc), "sql", as.character(sql))
  df_from_sdf(sc, sdf, arrow = arrow)
}

df_from_sdf <- function(sc, sdf, take = -1, arrow = TRUE) {
  sdf_collect(sdf, arrow = arrow)
}

#' Spark DataFrame from SQL
#'
#' Defines a Spark DataFrame from a SQL query, useful to create Spark DataFrames
#' without collecting the results immediately.
#'
#' @param sc A \code{spark_connection}.
#' @param sql a 'SQL' query used to generate a Spark DataFrame.
#'
#' @export
sdf_sql <- function(sc, sql) {
  hive_context(sc) %>%
    invoke("sql", sql) %>%
    sdf_register()
}
