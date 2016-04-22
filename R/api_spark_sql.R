spark_api_sql_tables <- function(con) {
  sqlResult <- spark_api_sql(con, "SHOW TABLES")
  spark_api_data_frame(con, sqlResult)
}

spark_api_sql_query <- function(con, query) {
  sqlResult <- spark_api_sql(con, as.character(query))
  df <- spark_api_data_frame(con, sqlResult)
}
