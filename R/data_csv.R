spark_csv_embedded_namespace <- function() {
  "com.databricks.spark.csv"
}

spark_csv_is_embedded <- function(sc) {
  invoke_static(
    sc,
    "sparklyr.Utils",
    "classExists",
    paste(spark_csv_embedded_namespace(), "CsvParser", sep = ".")
  )
}

spark_csv_is_loaded <- function(sc) {
  if (spark_version(sc) >= "2.0.0")
    TRUE
  else {
    spark_csv_is_embedded(sc)
  }
}

spark_csv_format_if_needed <- function(source, sc) {
  if (spark_csv_is_embedded(sc))
    invoke(source, "format", spark_csv_embedded_namespace())
  else
    source
}

spark_csv_load_name <- function(sc) {
  if (spark_csv_is_embedded(sc)) "load" else "csv"
}

spark_csv_save_name <- function(sc) {
  if (spark_csv_is_embedded(sc)) "save" else "csv"
}

spark_csv_read <- function(sc,
                           path,
                           csvOptions = list(),
                           columns = NULL) {
  read <- invoke(hive_context(sc), "read")

  options <- spark_csv_format_if_needed(read, sc)

  if (sparklyr_boolean_option("sparklyr.verbose") && !identical(columns, NULL)) {
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
    spark_csv_load_name(sc),
    path)
}

spark_csv_write <- function(df, path, csvOptions, mode, partition_by) {
  sc <- spark_connection(df)

  write <- invoke(df, "write")
  options <- spark_csv_format_if_needed(write, sc)

  lapply(names(csvOptions), function(csvOptionName) {
    options <<- invoke(options, "option", csvOptionName, csvOptions[[csvOptionName]])
  })

  if (!is.null(partition_by))
    options <- invoke(options, "partitionBy", as.list(partition_by))

  options <- spark_data_apply_mode(options, mode)

  invoke(
    options,
    spark_csv_save_name(sc),
    path)

  invisible(TRUE)
}
