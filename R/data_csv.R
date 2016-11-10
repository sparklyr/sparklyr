spark_csv_namespace <- function(sc) {
  if (spark_version(sc) >= "2.0.0")
    "org.apache.spark.sql.execution.datasources.csv"
  else
    "com.databricks.spark.csv"
}

spark_csv_class <- function(sc) {
  paste(spark_csv_namespace(sc), "CsvReader", sep = ".")
}

spark_csv_format_if_needed <- function(source, sc) {
  if (spark_version(sc) >= "2.0.0")
    source
  else
    invoke(source, "format", spark_csv_namespace(sc))
}

spark_csv_load_name <- function(sc) {
  if (spark_version(sc) >= "2.0.0")
    "csv"
  else
    "load"
}

spark_csv_read <- function(sc,
                           path,
                           csvOptions = list(),
                           columns = NULL) {
  read <- invoke(hive_context(sc), "read")

  options <- spark_csv_format_if_needed(read, sc)

  if (!identical(columns, NULL)) {
    ncol_ds <- options %>%
      invoke(spark_csv_load_name(sc), path) %>%
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

  invoke(
    optionSchema,
    if (spark_version(sc) >= "2.0.0") "csv" else "load",
    path)
}

spark_csv_write <- function(df, path, csvOptions) {
  write <- invoke(df, "write")
  options <- spark_csv_format_if_needed(write, sc)

  lapply(names(csvOptions), function(csvOptionName) {
    options <<- invoke(options, "option", csvOptionName, csvOptions[[csvOptionName]])
  })

  invoke(options, "save", path)

  invisible(TRUE)
}
