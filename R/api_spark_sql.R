spark_api_sql_tables <- function(api) {
  sqlResult <- spark_api_sql(api, "SHOW TABLES")
  spark_api_data_frame(api, sqlResult)
}

spark_api_sql_query <- function(api, query) {
  sqlResult <- spark_api_sql(api, as.character(query))
  spark_api_data_frame(api, sqlResult)
}
