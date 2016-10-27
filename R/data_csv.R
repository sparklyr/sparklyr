spark_csv_read <- function(sc,
                           path,
                           csvOptions = list(),
                           columns = NULL) {
  read <- invoke(hive_context(sc), "read")
  options <- invoke(read, "format", "com.databricks.spark.csv")

  if (!identical(columns, NULL)) {
    ncol_ds <- options %>%
      invoke("load", path) %>%
      invoke("schema") %>%
      invoke("length")

    if (ncol_ds != length(columns)) {
      warning("Dataset has ", ncol_ds, " columns but 'columns' has length ", length(columns))
    }
  }

  lapply(names(csvOptions), function(csvOptionName) {
    options <<- invoke(options, "option", csvOptionName, csvOptions[[csvOptionName]])
  })

  if (identical(columns, NULL)) {
    optionSchema <- options
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
