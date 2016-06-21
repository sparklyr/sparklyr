spark_api_read_csv <- function(api, path, columns = NULL) {
  read <- sparkapi_invoke(spark_sql_or_hive(api), "read")
  format <- sparkapi_invoke(read, "format", "com.databricks.spark.csv")
  optionHeader <- sparkapi_invoke(format, "option", "header", "true")

  if (identical(columns, NULL)) {
    optionSchema <- sparkapi_invoke(optionHeader, "option", "inferSchema", "true")
  }
  else {
    columnDefs <- spark_api_build_types(api, columns)
    optionSchema <- sparkapi_invoke(optionHeader, "schema", columnDefs)
  }

  sparkapi_invoke(optionSchema, "load", path)
}

spark_api_write_csv <- function(df, path) {
  write <- sparkapi_invoke(df, "write")
  format <- sparkapi_invoke(write, "format", "com.databricks.spark.csv")
  optionHeader <- sparkapi_invoke(format, "option", "header", "true")
  sparkapi_invoke(optionHeader, "save", path)

  invisible(TRUE)
}
