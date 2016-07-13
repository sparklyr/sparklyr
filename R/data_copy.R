
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

  # Spark unfortunately has a number of issues with '.'s in column names, e.g.
  #
  #    https://issues.apache.org/jira/browse/SPARK-5632
  #    https://issues.apache.org/jira/browse/SPARK-13455
  #
  # Many of these issues are marked as resolved, but it appears this is
  # a common regression in Spark and the handling is not uniform across
  # the Spark API.
  reNotAlpha <- "[^a-zA-Z0-9]"
  badNamesIdx <- grep(reNotAlpha, names(df))
  if (length(badNamesIdx)) {
    oldNames <- names(df)[badNamesIdx]
    newNames <- gsub(reNotAlpha, "_", oldNames)
    names(df)[badNamesIdx] <- newNames
    if (isTRUE(getOption("sparklyr.verbose", TRUE))) {

      nLhs <- max(nchar(oldNames))
      nRhs <- max(nchar(newNames))

      lhs <- sprintf(paste("%-", nLhs + 2, "s", sep = ""), shQuote(oldNames))
      rhs <- sprintf(paste("%-", nRhs + 2, "s", sep = ""), shQuote(newNames))

      msg <- paste(
        "The following columns have been renamed:",
        paste("-", lhs, "=>", rhs, collapse = "\n"),
        sep = "\n"
      )

      message(msg)
    }
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
