
df_from_sql <- function(sc, sql) {
  sdf <- invoke(hive_context(sc), "sql", as.character(sql))
  df_from_sdf(sc, sdf)
}

df_from_sdf <- function(sc, sdf, take = -1) {
  collect(sdf)
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
    invoke("sql", as.character(sql)) %>%
    sdf_register()
}
