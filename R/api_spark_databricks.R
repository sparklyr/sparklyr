spark_read_csv <- function(con, path, columns = NULL) {
  read <- spark_api(con, FALSE, con$sql$id, "read")
  format <- spark_api(con, FALSE, read$id, "format", "com.databricks.spark.csv")
  optionHeader <- spark_api(con, FALSE, format$id, "option", "header", "true")

  if (identical(columns, NULL)) {
    optionSchema <- spark_api(con, FALSE, optionHeader$id, "option", "inferSchema", "true")
  }
  else {
    columnDefs <- spark_read_csv_types(con, columns)
    optionSchema <- spark_api(con, FALSE, optionHeader$id, "schema", columnDefs)
  }

  df <- spark_api(con, FALSE, optionSchema$id, "load", path)

  df
}

spark_read_csv_types <- function(con, columns) {
  names <- names(columns)
  fields <- lapply(names, function(name) {
    spark_api(con, TRUE, "org.apache.spark.sql.api.r.SQLUtils", "createStructField", name, columns[[name]], TRUE)
  })

  spark_api(con, TRUE, "org.apache.spark.sql.api.r.SQLUtils", "createStructType", fields)
}
