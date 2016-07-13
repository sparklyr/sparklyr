
spark_data_build_types <- function(sc, columns) {
  names <- names(columns)
  fields <- lapply(names, function(name) {
    invoke_static(sc, "org.apache.spark.sql.api.r.SQLUtils", "createStructField", name, columns[[name]], TRUE)
  })

  invoke_static(sc, "org.apache.spark.sql.api.r.SQLUtils", "createStructType", fields)
}

spark_data_copy <- function(sc, df, name, repartition, local_file = TRUE) {
  if (!is.numeric(repartition)) {
    stop("The repartition parameter must be an integer")
  }

  # Escaping issues that used to work were broken in Spark 2.0.0-preview, fix:
  version <- invoke(spark_context(sc), "version")
  if (identical(version, "2.0.0-preview")) {
    names(df) <- gsub("[^a-zA-Z0-9]", "_", names(df))
  }

  columns <- lapply(df, function(e) {
    if (is.factor(e))
      "character"
    else
      typeof(e)
  })

  if (local_file) {
    tempfile <- tempfile(fileext = ".csv")
    write.csv(df, tempfile, row.names = FALSE, na = "")
    df <- spark_csv_read(sc, tempfile, csvOptions = list(
      header = "true"
    ), columns = columns)

    if (repartition > 0) {
      df <- invoke(df, "repartition", as.integer(repartition))
    }
  } else {
    structType <- spark_data_build_types(sc, columns)

    # Map date and time columns as standard doubles
    df <- as.data.frame(lapply(df, function(e) {
      if (is.time(e) || is.date(e))
        sapply(e, function(t) {
          class(t) <- NULL
          t
        })
      else
        e
    }), optional = TRUE)

    rows <- lapply(seq_len(NROW(df)), function(e) as.list(df[e,]))

    rdd <- invoke_static(
      sc,
      "utils",
      "createDataFrame",
      spark_context(sc),
      rows,
      as.integer(if (repartition <= 0) 1 else repartition)
    )

    df <- invoke(hive_context(sc), "createDataFrame", rdd, structType)
  }

  invoke(df, "registerTempTable", name)
}
