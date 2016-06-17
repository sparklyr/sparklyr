spark_api_read_csv <- function(api, path, columns = NULL) {
  read <- spark_invoke(spark_sql_or_hive(api), "read")
  format <- spark_invoke(read, "format", "com.databricks.spark.csv")
  optionHeader <- spark_invoke(format, "option", "header", "true")

  if (identical(columns, NULL)) {
    optionSchema <- spark_invoke(optionHeader, "option", "inferSchema", "true")
  }
  else {
    columnDefs <- spark_api_build_types(api, columns)
    optionSchema <- spark_invoke(optionHeader, "schema", columnDefs)
  }

  spark_invoke(optionSchema, "load", path)
}

spark_api_write_csv <- function(df, path) {
  write <- spark_invoke(df, "write")
  format <- spark_invoke(write, "format", "com.databricks.spark.csv")
  optionHeader <- spark_invoke(format, "option", "header", "true")
  spark_invoke(optionHeader, "save", path)

  invisible(TRUE)
}
