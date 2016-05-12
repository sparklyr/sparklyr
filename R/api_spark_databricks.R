spark_read_csv <- function(api, path, columns = NULL) {
  read <- spark_invoke(api$scon, spark_sql_or_hive(api), "read")
  format <- spark_invoke(api$scon, read, "format", "com.databricks.spark.csv")
  optionHeader <- spark_invoke(api$scon, format, "option", "header", "true")

  if (identical(columns, NULL)) {
    optionSchema <- spark_invoke(api$scon, optionHeader, "option", "inferSchema", "true")
  }
  else {
    columnDefs <- spark_read_csv_types(api, columns)
    optionSchema <- spark_invoke(api$scon, optionHeader, "schema", columnDefs)
  }

  df <- spark_invoke(api$scon, optionSchema, "load", path)

  df
}

spark_read_csv_types <- function(api, columns) {
  names <- names(columns)
  fields <- lapply(names, function(name) {
    spark_invoke_static(api$scon, "org.apache.spark.sql.api.r.SQLUtils", "createStructField", name, columns[[name]], TRUE)
  })

  spark_invoke_static(api$scon, "org.apache.spark.sql.api.r.SQLUtils", "createStructType", fields)
}
