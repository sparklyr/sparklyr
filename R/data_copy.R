#' @include spark_data_build_types.R
#' @include sql_utils.R
#' @include utils.R

spark_serialize_rds <- function(sc, df, columns, repartition) {
  num_rows <- nrow(df)
  timestamp_col_idxes <- Filter(
    function(i) inherits(df[[i + 1L]], "POSIXt"), seq(ncol(df)) - 1L
  )
  string_col_idxes <- Filter(
    function(i) inherits(df[[i + 1L]], c("character", "factor")), seq(ncol(df)) - 1L
  )
  cols <- lapply(df, function(x) {
    as.list(
      if (inherits(x, "Date")) {
        as.integer(x)
      } else if (inherits(x, "POSIXt")) {
        as.numeric(x)
      } else if (inherits(x, "factor")) {
        as.character(x)
      } else {
        x
      }
    )
  })
  rdd <- invoke_static(
    sc,
    "sparklyr.Utils",
    "parallelize",
    spark_context(sc),
    num_rows,
    cols %>%
      unname() %>%
      lapply(
        function(x) {
          serialize(x, connection = NULL, version = 2L, xdr = TRUE)
        }
      ),
    as.list(timestamp_col_idxes),
    as.list(string_col_idxes),
    if (repartition > 0) as.integer(repartition) else 1L
  )
  schema <- spark_data_build_types(sc, columns)

  invoke_static(
    sc,
    "org.apache.spark.sql.SQLUtils",
    "createDataFrame",
    hive_context(sc),
    rdd,
    schema
  )
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

spark_data_perform_copy <- function(sc, serializer, df_data, repartition, raw_columns = list()) {
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
    for (row_column in raw_columns) {
      columns[[row_column]] <- "raw"
    }
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
                            struct_columns = list(),
                            raw_columns = list()) {
  if (!is.numeric(repartition)) {
    stop("The repartition parameter must be an integer")
  }

  if (!spark_connection_is_local(sc) && identical(serializer, "csv_file")) {
    stop("Using a local file to copy data is not supported for remote clusters")
  }

  if (length(struct_columns) > 0 && spark_version(sc) < "2.4") {
    stop("Parameter 'struct_columns' requires Spark 2.4+")
  }

  temporal_array_columns <- new.env(parent = emptyenv())
  temporal_array_columns$date_array_columns <- list()
  temporal_array_columns$timestamp_array_columns <- list()
  additional_struct_columns <- list()
  additional_raw_columns <- list()

  if (spark_version(sc) >= "3.0") {
    process_column <- function(col) {
      for (arr in df[[col]]) {
        if (is.null(arr)) {
          next
        }
        if ("Date" %in% class(arr)) {
          temporal_array_columns$date_array_columns <- append(
            temporal_array_columns$date_array_columns, list(col)
          )
          break
        }
        if ("POSIXct" %in% class(arr)) {
          temporal_array_columns$timestamp_array_columns <- append(
            temporal_array_columns$date_array_columns, list(col)
          )
          break
        }
        # If current element is not NULL and is not an array of temporal values
        # then we assume the rest of the column does not contain array of
        # temporal values either
        break
      }
    }
    for (column in colnames(df)) {
      if ("list" %in% class(df[[column]])) {
        process_column(column)
      }
    }
  }

  for (column in colnames(df)) {
    if ("list" %in% class(df[[column]])) {
      if ("raw" %in% lapply(df[[column]], class)) {
        additional_raw_columns <- append(additional_raw_columns, column)
      } else {
        df[[column]] <- sapply(
          df[[column]],
          function(e) {
            jsonlite::toJSON(
              as.list(e),
              na = "null",
              auto_unbox = TRUE,
              digits = NA
            )
          }
        )
        additional_struct_columns <- append(additional_struct_columns, column)
      }
    }
  }
  struct_columns <- union(struct_columns, additional_struct_columns)
  raw_columns <- union(raw_columns, additional_raw_columns)

  serializer <- serializer %||% ifelse(arrow_enabled(sc, df), "arrow", "rds")

  serializers <- list(
    "rds" = spark_serialize_rds,
    "arrow" = spark_serialize_arrow
  )


  df <- spark_data_perform_copy(sc, serializers[[serializer]], df, repartition, raw_columns)

  if (length(struct_columns) > 0 && spark_version(sc) >= "2.4") {
    df <- invoke_static(
      sc,
      "sparklyr.StructColumnUtils",
      "parseJsonColumns",
      df,
      struct_columns
    )
  }

  if ((length(temporal_array_columns$date_array_columns) > 0 ||
    length(temporal_array_columns$timestamp_array_columns) > 0) &&
    spark_version(sc) >= "3.0") {
    df <- invoke_static(
      sc,
      "sparklyr.TemporalArrayColumnUtils",
      "transformTemporalArrayColumns",
      df,
      temporal_array_columns$date_array_columns,
      temporal_array_columns$timestamp_array_columns
    )
  }

  if (spark_version(sc) < "2.0.0") {
    invoke(df, "registerTempTable", name)
  } else {
    invoke(df, "createOrReplaceTempView", name)
  }

  df
}
