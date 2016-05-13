spark_read_csv <- function(api, path, columns = NULL) {
  optionHeader <- spark_sql_or_hive(api) %>%
    spark_invoke("read") %>%
    spark_invoke("format", "com.databricks.spark.csv") %>%
    spark_invoke("option", "header", "true")

  if (identical(columns, NULL)) {
    optionSchema <- spark_invoke(optionHeader, "option", "inferSchema", "true")
  }
  else {
    columnDefs <- spark_read_csv_types(api, columns)
    optionSchema <- spark_invoke(optionHeader, "schema", columnDefs)
  }

  spark_invoke(optionSchema, "load", path)
}

spark_read_csv_types <- function(api, columns) {
  names <- names(columns)
  fields <- lapply(names, function(name) {
    spark_invoke_static(api$scon, "org.apache.spark.sql.api.r.SQLUtils", "createStructField", name, columns[[name]], TRUE)
  })

  spark_invoke_static(api$scon, "org.apache.spark.sql.api.r.SQLUtils", "createStructType", fields)
}
