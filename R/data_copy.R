#' @include spark_data_build_types.R

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
    columns = columns
  )

  if (repartition > 0) {
    df <- invoke(df, "repartition", as.integer(repartition))
  }

  df
}

spark_serialize_csv_string <- function(sc, df, columns, repartition) {
  structType <- spark_data_build_types(sc, columns)

  # Map date and time columns as standard doubles or strings
  df <- as.data.frame(lapply(df, function(e) {
    if (inherits(e, "POSIXt")) {
      sapply(e, function(t) {
        class(t) <- NULL
        t
      })
    } else if (inherits(e, "Date")) {
      sapply(e, function(t) {
        as.character(t)
      })
    } else {
      e
    }
  }), optional = TRUE)

  separator <- split_separator(sc)

  tempFile <- tempfile(fileext = ".csv")
  write.table(df, tempFile, sep = separator$plain, col.names = FALSE, row.names = FALSE, quote = FALSE)
  textData <- as.list(readLines(tempFile))

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

  # Map date and time columns as standard doubles or strings
  df <- as.data.frame(lapply(df, function(e) {
    if (inherits(e, "POSIXt")) {
      sapply(e, function(t) {
        class(t) <- NULL
        t
      })
    } else if (inherits(e, "Date")) {
      sapply(e, function(t) {
        as.character(t)
      })
    } else {
      e
    }
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

spark_serialize_arrow <- function(sc, df, columns, repartition) {
  arrow_copy_to(
    sc,
    df,
    as.integer(if (repartition <= 0) 1 else repartition)
  )
}

spark_data_translate_columns <- function(df) {
  lapply(df, function(e) {
    if (is.factor(e)) {
      "character"
    } else if ("POSIXct" %in% class(e)) {
      "timestamp"
    } else if (inherits(e, "Date")) {
      "date"
    } else {
      typeof(e)
    }
  })
}

spark_data_perform_copy <- function(sc, serializer, df_data, repartition) {
  if (identical(class(df_data), "iterator")) {
    df <- df_data()
  }
  else {
    df_list <- df_data
    if (!identical(class(df_data), "list")) {
      df_list <- list(df_data)
    }
    df <- df_list[[1]]
  }

  # load arrow file in scala
  sdf_list <- list()
  i <- 1
  while (!is.null(df)) {
    if (is.function(df)) df <- df()
    if (is.language(df)) df <- rlang::as_closure(df)()

    # ensure data.frame
    if (!is.data.frame(df)) df <- sdf_prepare_dataframe(df)

    names(df) <- spark_sanitize_names(names(df), sc$config)
    columns <- spark_data_translate_columns(df)
    sdf_current <- serializer(sc, df, columns, repartition)
    sdf_list[[i]] <- sdf_current

    i <- i + 1
    if (identical(class(df_data), "iterator")) {
      df <- df_data()
    }
    else {
      df <- if (i <= length(df_list)) df_list[[i]] else NULL
    }

    # if more than one batch, partially cache results
    if (i > 2 || !is.null(df)) {
      invoke(sdf_current, "cache")
      sdf_count <- invoke(sdf_current, "count")
      if (spark_config_value(sc$config, "sparklyr.verbose", FALSE)) {
        message("Copied batch with ", sdf_count, " rows to Spark.")
      }
    }
  }

  sdf <- sdf_list[[1]]
  if (length(sdf_list) > 1) {
    rdd_list <- NULL
    for (i in seq_along(sdf_list)) {
      rdd_list[[i]] <- invoke(sdf_list[[i]], "rdd")
    }

    rdd <- invoke_static(sc, "sparklyr.Utils", "unionRdd", spark_context(sc), rdd_list)
    schema <- invoke(sdf_list[[1]], "schema")
    sdf <- invoke(hive_context(sc), "createDataFrame", rdd, schema)
  }

  sdf
}

spark_data_copy <- function(
                            sc,
                            df,
                            name,
                            repartition,
                            serializer = NULL,
                            struct_columns = list()) {
  if (!is.numeric(repartition)) {
    stop("The repartition parameter must be an integer")
  }

  if (!spark_connection_is_local(sc) && identical(serializer, "csv_file")) {
    stop("Using a local file to copy data is not supported for remote clusters")
  }

  if (length(struct_columns) > 0 && spark_version(sc) < "2.4") {
    stop("Parameter 'struct_columns' requires Spark 2.4+")
  }

  additional_struct_columns <- list()
  if ("list" %in% sapply(df, class)) {
    for (column in colnames(df)) {
      if (class(df[[column]]) == "list") {
        df[[column]] <- sapply(df[[column]], function(e) rjson::toJSON(as.list(e)))
        additional_struct_columns <- append(additional_struct_columns, column)
      }
    }
  }
  struct_columns <- union(struct_columns, additional_struct_columns)

  serializer <- ifelse(
    is.null(serializer),
    ifelse(
      arrow_enabled(sc, df),
      "arrow",
      ifelse(
        spark_connection_in_driver(sc),
        "csv_file_scala",
        getOption("sparklyr.copy.serializer", "csv_string")
      )
    ),
    serializer
  )

  serializers <- list(
    "csv_file" = spark_serialize_csv_file,
    "csv_string" = spark_serialize_csv_string,
    "csv_file_scala" = spark_serialize_csv_scala,
    "arrow" = spark_serialize_arrow
  )


  df <- spark_data_perform_copy(sc, serializers[[serializer]], df, repartition)

  if (length(struct_columns) > 0 && spark_version(sc) >= "2.4") {
    df <- invoke_static(
      sc,
      "sparklyr.StructColumnUtils",
      "parseJsonColumns",
      df,
      struct_columns
    )
  }

  if (spark_version(sc) < "2.0.0") {
    invoke(df, "registerTempTable", name)
  } else {
    invoke(df, "createOrReplaceTempView", name)
  }
}
