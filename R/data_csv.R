spark_csv_read <- function(sc,
                           path,
                           csvOptions = list(),
                           columns = NULL) {
  read <- invoke(hive_context(sc), "read")
  options <- invoke(read, "format", "com.databricks.spark.csv")

  lapply(names(csvOptions), function(csvOptionName) {
    options <<- invoke(options, "option", csvOptionName, csvOptions[[csvOptionName]])
  })

  if (identical(columns, NULL)) {
    optionSchema <- invoke(options, "option", "inferSchema", "true")
  }
  else {
    columnDefs <- spark_data_build_types(sc, columns)
    optionSchema <- invoke(options, "schema", columnDefs)
  }

  invoke(optionSchema, "load", path)
}

spark_csv_write <- function(df, path, csvOptions) {
  write <- invoke(df, "write")
  options <- invoke(write, "format", "com.databricks.spark.csv")

  lapply(names(csvOptions), function(csvOptionName) {
    options <<- invoke(options, "option", csvOptionName, csvOptions[[csvOptionName]])
  })

  invoke(options, "save", path)

  invisible(TRUE)
}
