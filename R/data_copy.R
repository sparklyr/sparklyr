
spark_data_build_types <- function(sc, columns) {
  names <- names(columns)
  fields <- lapply(names, function(name) {
    invoke_static(sc, "sparklyr.SQLUtils", "createStructField", name, columns[[name]], TRUE)
  })

  invoke_static(sc, "sparklyr.SQLUtils", "createStructType", fields)
}

spark_serialize_csv_file <- function(sc, df, columns, repartition) {

  # generate a CSV file from the associated data frame
  # note that these files need to live for the R session
  # duration so we don't clean these up eagerly
  # write file based on hash to avoid writing too many files
  # on repeated import calls
  hash <- digest::digest(df, algo = "sha256")
  filename <- paste("spark_serialize_", hash, ".csv", sep = "")
  tempfile <- file.path(tempdir(), filename)

  if (!file.exists(tempfile)) {
    write.csv(df, tempfile, row.names = FALSE, na = "")
  }

  df <- spark_csv_read(
    sc,
    paste("file:///", tempfile, sep = ""),
    csvOptions = list(
      header = "true"
    ),
    columns = columns)

  if (repartition > 0) {
    df <- invoke(df, "repartition", as.integer(repartition))
  }

  df
}

spark_serialize_csv_string <- function(sc, df, columns, repartition) {
  structType <- spark_data_build_types(sc, columns)

  # Map date and time columns as standard doubles
  df <- as.data.frame(lapply(df, function(e) {
    if (inherits(e, "POSIXt") || inherits(e, "Date"))
      sapply(e, function(t) {
        class(t) <- NULL
        t
      })
    else
      e
  }), optional = TRUE)

  separator <- split_separator(sc)

  tempFile <- tempfile(fileext = ".csv")
  write.table(df, tempFile, sep = separator$plain, col.names = FALSE, row.names = FALSE, quote = FALSE)
  textData <- as.list(readr::read_lines(tempFile))

  rdd <- invoke_static(
    sc,
    "sparklyr.Utils",
    "createDataFrameFromText",
    spark_context(sc),
    textData,
    columns,
    as.integer(if (repartition <= 0) 1 else repartition),
    separator$regexp
  )

  invoke(hive_context(sc), "createDataFrame", rdd, structType)
}

spark_serialize_csv_scala <- function(sc, df, columns, repartition) {
  structType <- spark_data_build_types(sc, columns)

  # Map date and time columns as standard doubles
  df <- as.data.frame(lapply(df, function(e) {
    if (inherits(e, "POSIXt") || inherits(e, "Date"))
      sapply(e, function(t) {
        class(t) <- NULL
        t
      })
    else
      e
  }), optional = TRUE)

  separator <- split_separator(sc)

  # generate a CSV file from the associated data frame
  # note that these files need to live for the R session
  # duration so we don't clean these up eagerly
  # write file based on hash to avoid writing too many files
  # on repeated import calls
  hash <- digest::digest(df, algo = "sha256")
  filename <- paste("spark_serialize_", hash, ".csv", sep = "")
  tempfile <- file.path(tempdir(), filename)

  if (!file.exists(tempfile)) {
    write.table(df, tempfile, sep = separator$plain, col.names = FALSE, row.names = FALSE, quote = FALSE)
  }

  rdd <- invoke_static(
    sc,
    "sparklyr.Utils",
    "createDataFrameFromCsv",
    spark_context(sc),
    tempfile,
    columns,
    as.integer(if (repartition <= 0) 1 else repartition),
    separator$regexp
  )

  invoke(hive_context(sc), "createDataFrame", rdd, structType)
}

spark_data_copy <- function(
  sc,
  df,
  name,
  repartition,
  serializer = getOption("sparklyr.copy.serializer", "csv_file")) {

  if (!is.numeric(repartition)) {
    stop("The repartition parameter must be an integer")
  }

  if (!spark_connection_is_local(sc) && identical(serializer, "csv_file")) {
    stop("Using a local file to copy data is not supported for remote clusters")
  }

  serializer <- ifelse(is.null(serializer),
                       ifelse(spark_connection_is_local(sc) ||
                              spark_connection_is_yarn_client(sc),
                              "csv_file_scala",
                              "csv_string"),
                       serializer)

  # Spark unfortunately has a number of issues with '.'s in column names, e.g.
  #
  #    https://issues.apache.org/jira/browse/SPARK-5632
  #    https://issues.apache.org/jira/browse/SPARK-13455
  #
  # Many of these issues are marked as resolved, but it appears this is
  # a common regression in Spark and the handling is not uniform across
  # the Spark API.
  names(df) <- spark_sanitize_names(names(df))

  columns <- lapply(df, function(e) {
    if (is.factor(e))
      "character"
    else if ("POSIXct" %in% class(e))
      "timestamp"
    else
      typeof(e)
  })

  serializers <- list(
    "csv_file" = spark_serialize_csv_file,
    "csv_string" = spark_serialize_csv_string,
    "csv_file_scala" = spark_serialize_csv_scala
  )

  df <- serializers[[serializer]](sc, df, columns, repartition)

  invoke(df, "registerTempTable", name)
}
