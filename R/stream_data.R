stream_read_generic_type <- function(sc,
                                     path,
                                     type,
                                     name,
                                     columns = NULL,
                                     stream_options = list())
{
  switch(type,
    csv = {
      spark_csv_read(
        sc,
        spark_normalize_path(path),
        stream_options,
        columns)
    },
    default = {
      spark_session(sc) %>%
        invoke("read") %>%
        invoke(type, path)
    }
  )
}

stream_read_generic <- function(sc,
                                path,
                                type,
                                name,
                                columns,
                                stream_options)
{
  schema <- NULL

  streamOptions <- spark_session(sc) %>%
    invoke("readStream")

  if (identical(columns, NULL)) {
    reader <- stream_read_generic_type(sc,
                                       path,
                                       type,
                                       name,
                                       columns = columns,
                                       stream_options = stream_options)
    schema <- invoke(reader, "schema")
  }
  else if ("spark_jobj" %in% class(columns)) {
    schema <- columns
  }
  else {
    schema <- spark_data_build_types(sc, columns)
  }

  for (optionName in names(stream_options)) {
    streamOptions <- invoke(streamOptions, "option", optionName, stream_options[[optionName]])
  }

  streamOptions %>%
    invoke("schema", schema) %>%
    invoke(type, path) %>%
    invoke("createOrReplaceTempView", name)

  tbl(sc, name)
}

stream_write_generic <- function(x, path, type, mode, trigger, checkpoint, stream_options)
{
  sdf <- spark_dataframe(x)
  sc <- spark_connection(x)
  mode <- match.arg(as.character(mode), choices = c("append", "complete", "update"))

  if (!invoke(sdf, "isStreaming"))
    stop("DataFrame requires streaming context. Use `stream_read_*()` to read from streams.")

  streamOptions <- invoke(sdf, "writeStream") %>%
    invoke("format", type)

  stream_options$path <- path

  stream_options$checkpointLocation <- checkpoint

  for (optionName in names(stream_options)) {
    streamOptions <- invoke(streamOptions, "option", optionName, stream_options[[optionName]])
  }

  trigger <- stream_trigger_create(trigger, sc)

  if (identical(type, "memory")) streamOptions <- invoke(streamOptions, "queryName", path)

  streamOptions %>%
    invoke("outputMode", mode) %>%
    invoke("trigger", trigger) %>%
    invoke("start") %>%
    stream_class() %>%
    stream_validate()
}

#' Read a CSV Stream into a Spark DataFrame
#'
#' Read a tabular data stream into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param name The name to assign to the newly generated stream.
#'
#' @family Spark stream serialization
#'
#' @export
stream_read_csv <- function(sc,
                            path,
                            name = NULL,
                            header = TRUE,
                            columns = NULL,
                            infer_schema = TRUE,
                            delimiter = ",",
                            quote = "\"",
                            escape = "\\",
                            charset = "UTF-8",
                            null_value = NULL,
                            options = list(),
                            ...)
{
  spark_require_version(sc, "2.0.0")

  name <- name %||% random_string("sparklyr_tmp_")

  streamOptions <- spark_csv_options(header = header,
                                     inferSchema = infer_schema,
                                     delimiter = delimiter,
                                     quote = quote,
                                     escape = escape,
                                     charset = charset,
                                     nullValue = null_value,
                                     options = options)

  stream_read_generic(sc,
                      path = path,
                      type = "csv",
                      name = name,
                      columns = columns,
                      stream_options = streamOptions)
}

#' Write a Spark DataFrame into CSV Stream
#'
#' Writes a Spark DataFrame to a tabular (typically, comma-separated) stream.
#'
#' @inheritParams spark_write_csv
#'
#' @param mode Specifies how data is written to a streaming sink. Valid values are
#'   \code{"append"}, \code{"complete"} or \code{"update"}.
#' @param trigger The trigger for the stream query, defaults to micro-batches runnnig
#'   every 5 seconds. See \code{\link{stream_trigger_interval}} and
#'   \code{\link{stream_trigger_continuous}}.
#' @param checkpoint The location where the system will write all the checkpoint.
#' information to guarantee end-to-end fault-tolerance.
#'
#' @family Spark stream serialization
#'
#' @export
stream_write_csv <- function(x,
                             path,
                             mode = c("append", "complete", "update"),
                             trigger = stream_trigger_interval(interval = 5000),
                             checkpoint = file.path(path, "checkpoint"),
                             header = TRUE,
                             delimiter = ",",
                             quote = "\"",
                             escape = "\\",
                             charset = "UTF-8",
                             null_value = NULL,
                             options = list(),
                             ...)
{
  spark_require_version(spark_connection(x), "2.0.0")

  streamOptions <- spark_csv_options(header = header,
                                     inferSchema = NULL,
                                     delimiter = delimiter,
                                     quote = quote,
                                     escape = escape,
                                     charset = charset,
                                     nullValue = null_value,
                                     options = options)

  stream_write_generic(x,
                       path = path,
                       type = "csv",
                       mode = mode,
                       trigger = trigger,
                       checkpoint = checkpoint,
                       stream_options = streamOptions)
}

#' Write a Spark DataFrame into Memory
#'
#' Writes a Spark DataFrame into memory.
#'
#' @inheritParams stream_write_csv
#'
#' @param name The name to use to register this stream.
#'
#' @family Spark stream serialization
#'
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local")
#'
#' dir.crate("iris-in")
#' write.csv(iris, "iris-in/iris.csv", row.names = FALSE)
#'
#' stream <- stream_read_csv(sc, "iris-in") %>% stream_write_memory()
#'
#' stop_stream(stream)
#'
#' }
#'
#' @export
stream_write_memory <- function(x,
                                name = random_string("sparklyr_tmp_"),
                                mode = c("append", "complete", "update"),
                                trigger = stream_trigger_interval(interval = 5000),
                                checkpoint = file.path("checkpoints", name, random_string("")),
                                options = list(),
                                ...)
{
  spark_require_version(spark_connection(x), "2.0.0")

  sc <- spark_connection(x)

  stream_write_generic(x,
                       path = name,
                       type = "memory",
                       mode = mode,
                       trigger = trigger,
                       checkpoint = checkpoint,
                       stream_options = options)
}

#' Read a Spark DataFrame Memory Stream
#'
#' Read a memory data stream into a Spark DataFrame.
#'
#' @inheritParams spark_read_csv
#' @param source The name of the memory stream to read from.
#' @param name The name to assign to the newly generated stream.
#'
#' @family Spark stream serialization
#'
#' @export
stream_read_memory <- function(sc,
                               source = NULL,
                               name = NULL,
                               header = TRUE,
                               options = list(),
                               ...)
{
  spark_require_version(sc, "2.0.0")

  columns <- spark_session(sc) %>%
    invoke("table", source) %>%
    invoke("schema")

  stream_read_generic(sc,
                      path = name,
                      type = "memory",
                      name = name,
                      columns = columns,
                      stream_options = options)
}
