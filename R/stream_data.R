#' @include spark_data_build_types.R
NULL

#' Read files created by the stream
#'
#' @inheritParams spark_read_csv
#' @param name The name to assign to the newly generated stream.
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local")
#'
#' dir.create("csv-in")
#' write.csv(iris, "csv-in/data.csv", row.names = FALSE)
#'
#' csv_path <- file.path("file://", getwd(), "csv-in")
#'
#' stream <- stream_read_csv(sc, csv_path) %>% stream_write_csv("csv-out")
#'
#' stream_stop(stream)
#' }
#'
#' @export
stream_read_csv <- function(
    sc,
    path,
    name = NULL,
    header = TRUE,
    columns = NULL,
    delimiter = ",",
    quote = "\"",
    escape = "\\",
    charset = "UTF-8",
    null_value = NULL,
    options = list(),
    ...) {
  infer_schema <- identical(columns, NULL) || is.character(columns)

  streamOptions <- spark_csv_options(
    header = header,
    inferSchema = infer_schema,
    delimiter = delimiter,
    quote = quote,
    escape = escape,
    charset = charset,
    nullValue = null_value,
    options = options
  )

  stream_read_generic(
    sc,
    path = path,
    type = "csv",
    name = name,
    columns = columns,
    stream_options = streamOptions
  )
}


#' Write files to the stream
#'
#' @inheritParams spark_write_csv
#'
#' @param mode Specifies how data is written to a streaming sink. Valid values are
#'   \code{"append"}, \code{"complete"} or \code{"update"}.
#' @param trigger The trigger for the stream query, defaults to micro-batches
#' running every 5 seconds. See \code{\link{stream_trigger_interval}} and
#'   \code{\link{stream_trigger_continuous}}.
#' @param checkpoint The location where the system will write all the checkpoint
#' information to guarantee end-to-end fault-tolerance.
#' @param partition_by Partitions the output by the given list of columns.
#' @family Spark stream serialization
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local")
#'
#' dir.create("csv-in")
#' write.csv(iris, "csv-in/data.csv", row.names = FALSE)
#'
#' csv_path <- file.path("file://", getwd(), "csv-in")
#'
#' stream <- stream_read_csv(sc, csv_path) %>% stream_write_csv("csv-out")
#'
#' stream_stop(stream)
#' }
#'
#' @export
stream_write_csv <- function(
    x,
    path,
    mode = c("append", "complete", "update"),
    trigger = stream_trigger_interval(),
    checkpoint = file.path(path, "checkpoint"),
    header = TRUE,
    delimiter = ",",
    quote = "\"",
    escape = "\\",
    charset = "UTF-8",
    null_value = NULL,
    options = list(),
    partition_by = NULL,
    ...) {
  streamOptions <- spark_csv_options(
    header = header,
    inferSchema = NULL,
    delimiter = delimiter,
    quote = quote,
    escape = escape,
    charset = charset,
    nullValue = null_value,
    options = options
  )

  stream_write_generic(x,
    path = path,
    type = "csv",
    mode = mode,
    trigger = trigger,
    checkpoint = checkpoint,
    partition_by = partition_by,
    stream_options = streamOptions
  )
}

#' @rdname stream_read_csv
#' @export
stream_read_text <- function(
    sc,
    path,
    name = NULL,
    options = list(),
    ...) {
  stream_read_generic(
    sc,
    path = path,
    type = "text",
    name = name,
    columns = list(line = "character"),
    stream_options = options
  )
}

#' @rdname stream_write_csv
#' @export
stream_write_text <- function(
    x,
    path,
    mode = c("append", "complete", "update"),
    trigger = stream_trigger_interval(),
    checkpoint = file.path(path, "checkpoints", random_string("")),
    options = list(),
    partition_by = NULL,
    ...) {
  stream_write_generic(
    x,
    path = path,
    type = "text",
    mode = mode,
    trigger = trigger,
    checkpoint = checkpoint,
    partition_by = partition_by,
    stream_options = options
  )
}

#' @rdname stream_read_csv
#' @export
stream_read_json <- function(
    sc,
    path,
    name = NULL,
    columns = NULL,
    options = list(),
    ...) {
  stream_read_generic(
    sc,
    path = path,
    type = "json",
    name = name,
    columns = columns,
    stream_options = options
  )
}

#' @rdname stream_write_csv
#' @export
stream_write_json <- function(
    x,
    path,
    mode = c("append", "complete", "update"),
    trigger = stream_trigger_interval(),
    checkpoint = file.path(path, "checkpoints", random_string("")),
    options = list(),
    partition_by = NULL,
    ...) {
  stream_write_generic(
    x,
    path = path,
    type = "json",
    mode = mode,
    trigger = trigger,
    checkpoint = checkpoint,
    partition_by = partition_by,
    stream_options = options
  )
}

#' @rdname stream_read_csv
#' @export
stream_read_parquet <- function(
    sc,
    path,
    name = NULL,
    columns = NULL,
    options = list(),
    ...) {
  stream_read_generic(
    sc,
    path = path,
    type = "parquet",
    name = name,
    columns = columns,
    stream_options = options
  )
}

#' @rdname stream_write_csv
#' @export
stream_write_parquet <- function(
    x,
    path,
    mode = c("append", "complete", "update"),
    trigger = stream_trigger_interval(),
    checkpoint = file.path(path, "checkpoints", random_string("")),
    options = list(),
    partition_by = NULL,
    ...) {
  stream_write_generic(
    x,
    path = path,
    type = "parquet",
    mode = mode,
    trigger = trigger,
    checkpoint = checkpoint,
    partition_by = partition_by,
    stream_options = options
  )
}

#' @rdname stream_read_csv
#' @export
stream_read_orc <- function(
    sc,
    path,
    name = NULL,
    columns = NULL,
    options = list(),
    ...) {
  stream_read_generic(
    sc,
    path = path,
    type = "orc",
    name = name,
    columns = columns,
    stream_options = options
  )
}

#' @rdname stream_write_csv
#' @export
stream_write_orc <- function(
    x,
    path,
    mode = c("append", "complete", "update"),
    trigger = stream_trigger_interval(),
    checkpoint = file.path(path, "checkpoints", random_string("")),
    options = list(),
    partition_by = NULL,
    ...) {
  stream_write_generic(
    x,
    path = path,
    type = "orc",
    mode = mode,
    trigger = trigger,
    checkpoint = checkpoint,
    partition_by = partition_by,
    stream_options = options
  )
}

#' @rdname stream_read_csv
#' @export
stream_read_kafka <- function(
    sc,
    name = NULL,
    options = list(),
    ...) {
  stream_read_generic(
    sc,
    path = NULL,
    type = "kafka",
    name = name,
    columns = FALSE,
    stream_options = options
  )
}

#' @rdname stream_write_csv
#' @export
stream_write_kafka <- function(
    x,
    mode = c("append", "complete", "update"),
    trigger = stream_trigger_interval(),
    checkpoint = file.path("checkpoints", random_string("")),
    options = list(),
    partition_by = NULL,
    ...) {
  stream_write_generic(
    x,
    path = NULL,
    type = "kafka",
    mode = mode,
    trigger = trigger,
    checkpoint = checkpoint,
    partition_by = partition_by,
    stream_options = options
  )
}

#' @rdname stream_read_csv
#' @export
stream_read_socket <- function(
    sc,
    name = NULL,
    columns = NULL,
    options = list(),
    ...) {
  stream_read_generic(
    sc,
    path = NULL,
    type = "socket",
    name = name,
    columns = FALSE,
    stream_options = options
  )
}

#' @rdname stream_write_csv
#' @export
stream_write_console <- function(
    x,
    mode = c("append", "complete", "update"),
    options = list(),
    trigger = stream_trigger_interval(),
    partition_by = NULL,
    ...) {
  stream_write_generic(x,
    path = NULL,
    type = "console",
    mode = mode,
    trigger = trigger,
    checkpoint = NULL,
    partition_by = partition_by,
    stream_options = options
  )
}

#' @rdname stream_read_csv
#' @export
stream_read_delta <- function(
    sc,
    path,
    name = NULL,
    options = list(),
    ...) {
  stream_read_generic(
    sc,
    path = path,
    type = "delta",
    name = name,
    columns = FALSE,
    stream_options = options,
    load = TRUE
  )
}

#' @rdname stream_write_csv
#' @export
stream_write_delta <- function(
    x,
    path,
    mode = c("append", "complete", "update"),
    checkpoint = file.path("checkpoints", random_string("")),
    options = list(),
    partition_by = NULL,
    ...) {
  stream_write_generic(x,
    path = path,
    type = "delta",
    mode = mode,
    trigger = FALSE,
    checkpoint = checkpoint,
    partition_by = partition_by,
    stream_options = options
  )
}

#' @rdname stream_read_csv
#' @export
stream_read_cloudfiles <- function(
    sc,
    path,
    name = NULL,
    options = list(),
    ...) {
  stream_read_generic(
    sc,
    path = path,
    type = "cloudFiles",
    name = name,
    columns = FALSE,
    stream_options = options,
    load = TRUE
  )
}

#' @rdname stream_read_csv
#' @export
stream_read_table <- function(
    sc,
    path,
    name = NULL,
    options = list(),
    ...) {
  stream_read_generic(
    sc,
    path = path,
    type = "table",
    name = name,
    columns = FALSE,
    stream_options = options,
    load = FALSE
  )
}


#' Write Stream to Table
#'
#' Writes a Spark dataframe stream into a table.
#'
#' @inheritParams stream_write_csv
#' @param format Specifies format of data written to table E.g.
#' \code{"delta"}, \code{"parquet"}. Defaults to \code{NULL} which will use
#' system default format.
#' @family Spark stream serialization
#' @export
stream_write_table <- function(
    x,
    path,
    format = NULL,
    mode = c("append", "complete", "update"),
    checkpoint = file.path("checkpoints", random_string("")),
    options = list(),
    partition_by = NULL,
    ...) {
  stream_write_generic(x,
                       path = path,
                       type = format,
                       mode = mode,
                       trigger = FALSE,
                       checkpoint = checkpoint,
                       partition_by = partition_by,
                       stream_options = options,
                       to_table = TRUE
  )
}

stream_read_generic_type <- function(
    sc,
    path,
    type,
    name,
    columns = NULL,
    stream_options = list()) {
  switch(type,
    csv = {
      spark_csv_read(
        sc,
        spark_normalize_path(path),
        stream_options,
        columns
      )
    },
    {
      spark_session(sc) %>%
        invoke("read") %>%
        invoke(type, path)
    }
  )
}

stream_read_generic <- function(
    sc,
    path,
    type,
    name,
    columns,
    stream_options,
    load = FALSE) {
  spark_require_version(sc, "2.0.0", "Spark streaming")
  name <- name %||% random_string("sparklyr_tmp_")
  schema <- NULL
  streamOptions <- spark_session(sc) %>%
    invoke("readStream")

  if (identical(columns, NULL)) {
    reader <- stream_read_generic_type(sc,
      path,
      type,
      name,
      columns = columns,
      stream_options = stream_options
    )
    schema <- invoke(reader, "schema")
  } else if ("spark_jobj" %in% class(columns)) {
    schema <- columns
  } else if (identical(columns, FALSE)) {
    schema <- NULL
  } else {
    schema <- spark_data_build_types(sc, columns)
  }

  for (optionName in names(stream_options)) {
    streamOptions <- invoke(streamOptions, "option", optionName, stream_options[[optionName]])
  }

  if (identical(schema, NULL)) {
    schema <- streamOptions
  } else {
    schema <- streamOptions %>%
      invoke("schema", schema)
  }

  if (is.null(path) || load) {
    schema <- schema %>%
      invoke("format", type)

    schema <- if (load) invoke(schema, "load", path) else invoke(schema, "load")

    schema %>%
      invoke("createOrReplaceTempView", name)
  } else {
    schema %>%
      invoke(type, path) %>%
      invoke("createOrReplaceTempView", name)
  }

  tbl(sc, name)
}

stream_write_generic <- function(
    x, path, type, mode, trigger, checkpoint, partition_by, stream_options, to_table = FALSE) {
  spark_require_version(spark_connection(x), "2.0.0", "Spark streaming")

  sdf <- spark_dataframe(x)
  sc <- spark_connection(x)
  mode <- match.arg(as.character(mode), choices = c("append", "complete", "update"))

  if (!sdf_is_streaming(sdf)) {
    stop(
      "DataFrame requires streaming context. Use `stream_read_*()` to read from streams."
    )
  }

  streamOptions <- invoke(sdf, "writeStream")

  if (!is.null(type)) {
    streamOptions <- streamOptions %>% invoke("format", type)
  }

  if (!is.null(partition_by)) {
    streamOptions <- streamOptions %>% invoke("partitionBy", as.list(partition_by))
  }

  # TODO: review if path should be permitted when sinking to table
  # if so, check for missing-ness or default path to null?
  if (!to_table) {
    stream_options$path <- path
  }

  stream_options$checkpointLocation <- checkpoint

  streamOptions <- invoke(
    streamOptions,
    "queryName",
    ifelse(
      identical(type, "memory"),
      path,
      random_string("stream_")
    )
  )

  for (optionName in names(stream_options)) {
    streamOptions <- invoke(
      streamOptions,
      "option",
      optionName,
      stream_options[[optionName]]
    )
  }

  if (!identical(trigger, FALSE)) {
    trigger <- stream_trigger_create(trigger, sc)
  }

  streamOptions <- streamOptions %>%
    invoke("outputMode", mode)

  if (!identical(trigger, FALSE)) {
    streamOptions <- streamOptions %>% invoke("trigger", trigger)
  }

  if (to_table) {
    streamOptions <- streamOptions %>% invoke("toTable", path)
  } else {
    streamOptions <- streamOptions %>% invoke("start")
  }

  streamOptions %>%
    stream_class() %>%
    stream_validate() %>%
    stream_register()
}

#' Write Memory Stream
#'
#' Writes a Spark dataframe stream into a memory stream.
#'
#' @inheritParams stream_write_csv
#' @param name The name to assign to the newly generated stream.
#' @param partition_by Partitions the output by the given list of columns.
#' @family Spark stream serialization
#' @export
stream_write_memory <- function(
    x,
    name = random_string("sparklyr_tmp_"),
    mode = c("append", "complete", "update"),
    trigger = stream_trigger_interval(),
    checkpoint = file.path("checkpoints", name, random_string("")),
    options = list(),
    partition_by = NULL,
    ...) {
  sc <- spark_connection(x)
  spark_require_version(sc, "2.0.0", "Spark streaming")

  if (missing(mode)) {
    mode <- "append"
    tryCatch(
      {
        queryPlan <- spark_dataframe(x) %>%
          invoke("queryExecution") %>%
          invoke("analyzed")
        tryMode <- invoke_static(sc, "org.apache.spark.sql.streaming.OutputMode", "Complete")
        invoke_static(
          sc,
          "org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker",
          "checkForStreaming",
          queryPlan,
          tryMode
        )

        mode <- "complete"
      },
      error = function(e) {

      }
    )
  }

  stream_write_generic(x,
    path = name,
    type = "memory",
    mode = mode,
    trigger = trigger,
    checkpoint = checkpoint,
    partition_by = partition_by,
    stream_options = options
  )
}
