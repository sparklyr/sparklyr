spark_api_sql <- function(sc, sql) {
  result <- invoke(
    hive_context(sc),
    "sql",
    sql
  )
  
  result
}

spark_api_sql_tables <- function(sc) {
  sqlResult <- spark_api_sql(sc, "SHOW TABLES")
  spark_api_data_frame(sc, sqlResult)
}

spark_api_sql_query <- function(sc, query) {
  sqlResult <- spark_api_sql(sc, as.character(query))
  spark_api_data_frame(sc, sqlResult)
}
