#' @include spark_data_build_types.R

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
    {
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
                                stream_options,
                                load = FALSE)
{
  spark_require_version(sc, "2.0.0", "Spark streaming")

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
  else if (identical(columns, FALSE)) {
    schema <- NULL
  }
  else {
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
  }
  else {
    schema %>%
      invoke(type, path) %>%
      invoke("createOrReplaceTempView", name)
  }

  tbl(sc, name)
}

stream_write_generic <- function(x, path, type, mode, trigger, checkpoint, stream_options)
{
  spark_require_version(spark_connection(x), "2.0.0", "Spark streaming")

  sdf <- spark_dataframe(x)
  sc <- spark_connection(x)
  mode <- match.arg(as.character(mode), choices = c("append", "complete", "update"))

  if (!sdf_is_streaming(sdf))
    stop("DataFrame requires streaming context. Use `stream_read_*()` to read from streams.")

  streamOptions <- invoke(sdf, "writeStream") %>%
    invoke("format", type)

  stream_options$path <- path

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
    streamOptions <- invoke(streamOptions, "option", optionName, stream_options[[optionName]])
  }

  if (!identical(trigger, FALSE)) {
    trigger <- stream_trigger_create(trigger, sc)
  }

  streamOptions <- streamOptions %>%
    invoke("outputMode", mode)

  if (!identical(trigger, FALSE)) {
    streamOptions <- streamOptions %>% invoke("trigger", trigger)
  }

  streamOptions %>%
    invoke("start") %>%
    stream_class() %>%
    stream_validate() %>%
    stream_register()
}

#' Read CSV Stream
#'
#' Reads a CSV stream as a Spark dataframe stream.
#'
#' @inheritParams spark_read_csv
#' @param name The name to assign to the newly generated stream.
#'
#' @family Spark stream serialization
#'
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
#'
#' }
#'
#' @export
stream_read_csv <- function(sc,
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
                            ...)
{
  spark_require_version(sc, "2.0.0", "Spark streaming")

  name <- name %||% random_string("sparklyr_tmp_")

  infer_schema <- identical(columns, NULL) || is.character(columns)

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

#' Write CSV Stream
#'
#' Writes a Spark dataframe stream into a tabular (typically, comma-separated) stream.
#'
#' @inheritParams spark_write_csv
#'
#' @param mode Specifies how data is written to a streaming sink. Valid values are
#'   \code{"append"}, \code{"complete"} or \code{"update"}.
#' @param trigger The trigger for the stream query, defaults to micro-batches runnnig
#'   every 5 seconds. See \code{\link{stream_trigger_interval}} and
#'   \code{\link{stream_trigger_continuous}}.
#' @param checkpoint The location where the system will write all the checkpoint
#' information to guarantee end-to-end fault-tolerance.
#'
#' @family Spark stream serialization
#'
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
#'
#' }
#'
#' @export
stream_write_csv <- function(x,
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
                             ...)
{
  spark_require_version(spark_connection(x), "2.0.0", "Spark streaming")

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

#' Write Memory Stream
#'
#' Writes a Spark dataframe stream into a memory stream.
#'
#' @inheritParams stream_write_csv
#'
#' @param name The name to assign to the newly generated stream.
#'
#' @family Spark stream serialization
#'
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
#' stream <- stream_read_csv(sc, csv_path) %>% stream_write_memory("csv-out")
#'
#' stream_stop(stream)
#'
#' }
#'
#' @export
stream_write_memory <- function(x,
                                name = random_string("sparklyr_tmp_"),
                                mode = c("append", "complete", "update"),
                                trigger = stream_trigger_interval(),
                                checkpoint = file.path("checkpoints", name, random_string("")),
                                options = list(),
                                ...)
{
  sc <- spark_connection(x)
  spark_require_version(sc, "2.0.0", "Spark streaming")

  if (missing(mode)) {
    mode <- "append"
    tryCatch({
      queryPlan <- spark_dataframe(x) %>% invoke("queryExecution") %>% invoke("analyzed")
      tryMode <- invoke_static(sc, "org.apache.spark.sql.streaming.OutputMode", "Complete")
      invoke_static(
        sc,
        "org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker",
        "checkForStreaming",
        queryPlan,
        tryMode
      )

      mode <- "complete"
    }, error = function(e){

    })
  }

  stream_write_generic(x,
                       path = name,
                       type = "memory",
                       mode = mode,
                       trigger = trigger,
                       checkpoint = checkpoint,
                       stream_options = options)
}

#' Read Text Stream
#'
#' Reads a text stream as a Spark dataframe stream.
#'
#' @inheritParams stream_read_csv
#'
#' @family Spark stream serialization
#'
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local")
#'
#' dir.create("text-in")
#' writeLines("A text entry", "text-in/text.txt")
#'
#' text_path <- file.path("file://", getwd(), "text-in")
#'
#' stream <- stream_read_text(sc, text_path) %>% stream_write_text("text-out")
#'
#' stream_stop(stream)
#'
#' }
#'
#' @export
stream_read_text <- function(sc,
                             path,
                             name = NULL,
                             options = list(),
                             ...)
{
  spark_require_version(sc, "2.0.0", "Spark streaming")

  name <- name %||% random_string("sparklyr_tmp_")

  stream_read_generic(sc,
                      path = path,
                      type = "text",
                      name = name,
                      columns = list(line = "character"),
                      stream_options = options)
}

#' Write Text Stream
#'
#' Writes a Spark dataframe stream into a text stream.
#'
#' @inheritParams stream_write_memory
#' @param path The destination path. Needs to be accessible from the cluster.
#'   Supports the \samp{"hdfs://"}, \samp{"s3a://"} and \samp{"file://"} protocols.
#'
#' @family Spark stream serialization
#'
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local")
#'
#' dir.create("text-in")
#' writeLines("A text entry", "text-in/text.txt")
#'
#' text_path <- file.path("file://", getwd(), "text-in")
#'
#' stream <- stream_read_text(sc, text_path) %>% stream_write_text("text-out")
#'
#' stream_stop(stream)
#'
#' }
#'
#' @export
stream_write_text <- function(x,
                              path,
                              mode = c("append", "complete", "update"),
                              trigger = stream_trigger_interval(),
                              checkpoint = file.path(path, "checkpoints", random_string("")),
                              options = list(),
                              ...)
{
  spark_require_version(spark_connection(x), "2.0.0", "Spark streaming")

  sc <- spark_connection(x)

  stream_write_generic(x,
                       path = path,
                       type = "text",
                       mode = mode,
                       trigger = trigger,
                       checkpoint = checkpoint,
                       stream_options = options)
}

#' Read JSON Stream
#'
#' Reads a JSON stream as a Spark dataframe stream.
#'
#' @inheritParams stream_read_csv
#'
#' @family Spark stream serialization
#'
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local")
#'
#' dir.create("json-in")
#' jsonlite::write_json(list(a = c(1,2), b = c(10,20)), "json-in/data.json")
#'
#' json_path <- file.path("file://", getwd(), "json-in")
#'
#' stream <- stream_read_json(sc, json_path) %>% stream_write_json("json-out")
#'
#' stream_stop(stream)
#'
#' }
#'
#' @export
stream_read_json <- function(sc,
                             path,
                             name = NULL,
                             columns = NULL,
                             options = list(),
                             ...)
{
  spark_require_version(sc, "2.0.0", "Spark streaming")

  name <- name %||% random_string("sparklyr_tmp_")

  stream_read_generic(sc,
                      path = path,
                      type = "json",
                      name = name,
                      columns = columns,
                      stream_options = options)
}

#' Write JSON Stream
#'
#' Writes a Spark dataframe stream into a JSON stream.
#'
#' @inheritParams stream_write_text
#'
#' @family Spark stream serialization
#'
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local")
#'
#' dir.create("json-in")
#' jsonlite::write_json(list(a = c(1,2), b = c(10,20)), "json-in/data.json")
#'
#' json_path <- file.path("file://", getwd(), "json-in")
#'
#' stream <- stream_read_json(sc, json_path) %>% stream_write_json("json-out")
#'
#' stream_stop(stream)
#'
#' }
#'
#' @export
stream_write_json <- function(x,
                              path,
                              mode = c("append", "complete", "update"),
                              trigger = stream_trigger_interval(),
                              checkpoint = file.path(path, "checkpoints", random_string("")),
                              options = list(),
                              ...)
{
  spark_require_version(spark_connection(x), "2.0.0", "Spark streaming")

  sc <- spark_connection(x)

  stream_write_generic(x,
                       path = path,
                       type = "json",
                       mode = mode,
                       trigger = trigger,
                       checkpoint = checkpoint,
                       stream_options = options)
}

#' Read Parquet Stream
#'
#' Reads a parquet stream as a Spark dataframe stream.
#'
#' @inheritParams stream_read_csv
#'
#' @family Spark stream serialization
#'
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local")
#'
#' sdf_len(sc, 10) %>% spark_write_parquet("parquet-in")
#'
#' stream <- stream_read_parquet(sc, "parquet-in") %>% stream_write_parquet("parquet-out")
#'
#' stream_stop(stream)
#'
#' }
#'
#' @export
stream_read_parquet <- function(sc,
                                path,
                                name = NULL,
                                columns = NULL,
                                options = list(),
                                ...)
{
  spark_require_version(sc, "2.0.0", "Spark streaming")

  name <- name %||% random_string("sparklyr_tmp_")

  stream_read_generic(sc,
                      path = path,
                      type = "parquet",
                      name = name,
                      columns = columns,
                      stream_options = options)
}

#' Write Parquet Stream
#'
#' Writes a Spark dataframe stream into a parquet stream.
#'
#' @inheritParams stream_write_text
#'
#' @family Spark stream serialization
#'
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local")
#'
#' sdf_len(sc, 10) %>% spark_write_parquet("parquet-in")
#'
#' stream <- stream_read_parquet(sc, "parquet-in") %>% stream_write_parquet("parquet-out")
#'
#' stream_stop(stream)
#'
#' }
#'
#' @export
stream_write_parquet <- function(x,
                                 path,
                                 mode = c("append", "complete", "update"),
                                 trigger = stream_trigger_interval(),
                                 checkpoint = file.path(path, "checkpoints", random_string("")),
                                 options = list(),
                                 ...)
{
  spark_require_version(spark_connection(x), "2.0.0", "Spark streaming")

  sc <- spark_connection(x)

  stream_write_generic(x,
                       path = path,
                       type = "parquet",
                       mode = mode,
                       trigger = trigger,
                       checkpoint = checkpoint,
                       stream_options = options)
}

#' Read ORC Stream
#'
#' Reads an \href{https://orc.apache.org/}{ORC} stream as a Spark dataframe stream.
#'
#' @inheritParams stream_read_csv
#'
#' @family Spark stream serialization
#'
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local")
#'
#' sdf_len(sc, 10) %>% spark_write_orc("orc-in")
#'
#' stream <- stream_read_orc(sc, "orc-in") %>% stream_write_orc("orc-out")
#'
#' stream_stop(stream)
#'
#' }
#'
#' @export
stream_read_orc <- function(sc,
                            path,
                            name = NULL,
                            columns = NULL,
                            options = list(),
                            ...)
{
  spark_require_version(sc, "2.0.0", "Spark streaming")

  name <- name %||% random_string("sparklyr_tmp_")

  stream_read_generic(sc,
                      path = path,
                      type = "orc",
                      name = name,
                      columns = columns,
                      stream_options = options)
}

#' Write a ORC Stream
#'
#' Writes a Spark dataframe stream into an \href{https://orc.apache.org/}{ORC} stream.
#'
#' @inheritParams stream_write_text
#'
#' @family Spark stream serialization
#'
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local")
#'
#' sdf_len(sc, 10) %>% spark_write_orc("orc-in")
#'
#' stream <- stream_read_orc(sc, "orc-in") %>% stream_write_orc("orc-out")
#'
#' stream_stop(stream)
#'
#' }
#'
#' @export
stream_write_orc <- function(x,
                             path,
                             mode = c("append", "complete", "update"),
                             trigger = stream_trigger_interval(),
                             checkpoint = file.path(path, "checkpoints", random_string("")),
                             options = list(),
                             ...)
{
  spark_require_version(spark_connection(x), "2.0.0", "Spark streaming")

  sc <- spark_connection(x)

  stream_write_generic(x,
                       path = path,
                       type = "orc",
                       mode = mode,
                       trigger = trigger,
                       checkpoint = checkpoint,
                       stream_options = options)
}

#' Read Kafka Stream
#'
#' Reads a Kafka stream as a Spark dataframe stream.
#'
#' @inheritParams stream_read_csv
#'
#' @details Please note that Kafka requires installing the appropriate
#' package by setting the \code{packages} parameter to \code{"kafka"} in \code{spark_connect()}
#'
#' @family Spark stream serialization
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "local", version = "2.3", packages = "kafka")
#'
#' read_options <- list(kafka.bootstrap.servers = "localhost:9092", subscribe = "topic1")
#' write_options <- list(kafka.bootstrap.servers = "localhost:9092", topic = "topic2")
#'
#' stream <- stream_read_kafka(sc, options = read_options) %>%
#'   stream_write_kafka(options = write_options)
#'
#' stream_stop(stream)
#'
#' }
#'
#' @export
stream_read_kafka <- function(sc,
                              name = NULL,
                              options = list(),
                              ...)
{
  spark_require_version(sc, "2.0.0", "Spark streaming")

  name <- name %||% random_string("sparklyr_tmp_")

  stream_read_generic(sc,
                      path = NULL,
                      type = "kafka",
                      name = name,
                      columns = FALSE,
                      stream_options = options)
}

#' Write Kafka Stream
#'
#' Writes a Spark dataframe stream into an kafka stream.
#'
#' @inheritParams stream_write_memory
#'
#' @details Please note that Kafka requires installing the appropriate
#'  package by setting the \code{packages} parameter to \code{"kafka"} in \code{spark_connect()}
#'
#' @family Spark stream serialization
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "local", version = "2.3", packages = "kafka")
#'
#' read_options <- list(kafka.bootstrap.servers = "localhost:9092", subscribe = "topic1")
#' write_options <- list(kafka.bootstrap.servers = "localhost:9092", topic = "topic2")
#'
#' stream <- stream_read_kafka(sc, options = read_options) %>%
#'   stream_write_kafka(options = write_options)
#'
#' stream_stop(stream)
#'
#' }
#'
#' @export
stream_write_kafka <- function(x,
                               mode = c("append", "complete", "update"),
                               trigger = stream_trigger_interval(),
                               checkpoint = file.path("checkpoints", random_string("")),
                               options = list(),
                               ...)
{
  spark_require_version(spark_connection(x), "2.0.0", "Spark streaming")

  sc <- spark_connection(x)

  stream_write_generic(x,
                       path = NULL,
                       type = "kafka",
                       mode = mode,
                       trigger = trigger,
                       checkpoint = checkpoint,
                       stream_options = options)
}

#' Read Socket Stream
#'
#' Reads a Socket stream as a Spark dataframe stream.
#'
#' @inheritParams stream_read_csv
#'
#' @family Spark stream serialization
#'
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local")
#'
#' # Start socket server from terminal, example: nc -lk 9999
#' stream <- stream_read_socket(sc, options = list(host = "localhost", port = 9999))
#' stream
#'
#' }
#'
#' @export
stream_read_socket <- function(sc,
                               name = NULL,
                               columns = NULL,
                               options = list(),
                               ...)
{
  spark_require_version(sc, "2.0.0", "Spark streaming")

  name <- name %||% random_string("sparklyr_tmp_")

  stream_read_generic(sc,
                      path = NULL,
                      type = "socket",
                      name = name,
                      columns = FALSE,
                      stream_options = options)
}

#' Write Console Stream
#'
#' Writes a Spark dataframe stream into console logs.
#'
#' @inheritParams stream_write_memory
#'
#' @family Spark stream serialization
#'
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local")
#'
#' sdf_len(sc, 10) %>% dplyr::transmute(text = as.character(id)) %>% spark_write_text("text-in")
#'
#' stream <- stream_read_text(sc, "text-in") %>% stream_write_console()
#'
#' stream_stop(stream)
#'
#' }
#'
#' @export
stream_write_console <- function(x,
                                 mode = c("append", "complete", "update"),
                                 options = list(),
                                 trigger = stream_trigger_interval(),
                                 ...)
{
  spark_require_version(spark_connection(x), "2.0.0", "Spark streaming")

  sc <- spark_connection(x)

  stream_write_generic(x,
                       path = NULL,
                       type = "console",
                       mode = mode,
                       trigger = trigger,
                       checkpoint = NULL,
                       stream_options = options)
}

#' Read Delta Stream
#'
#' Reads a Delta Lake table as a Spark dataframe stream.
#'
#' @inheritParams stream_read_csv
#'
#' @details Please note that Delta Lake requires installing the appropriate
#' package by setting the \code{packages} parameter to \code{"delta"} in \code{spark_connect()}
#'
#' @family Spark stream serialization
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "local", version = "2.4", packages = "delta")
#'
#' sdf_len(sc, 5) %>% spark_write_delta(path = "delta-test")
#'
#' stream <- stream_read_delta(sc, "delta-test") %>%
#'   stream_write_json("json-out")
#'
#' stream_stop(stream)
#'
#' }
#'
#' @export
stream_read_delta <- function(sc,
                              path,
                              name = NULL,
                              options = list(),
                              ...)
{
  spark_require_version(sc, "2.0.0", "Spark streaming")

  name <- name %||% random_string("sparklyr_tmp_")

  stream_read_generic(sc,
                      path = path,
                      type = "delta",
                      name = name,
                      columns = FALSE,
                      stream_options = options,
                      load = TRUE)
}

#' Write Delta Stream
#'
#' Writes a Spark dataframe stream into a Delta Lake table.
#'
#' @inheritParams stream_write_csv
#'
#' @details Please note that Delta Lake requires installing the appropriate
#' package by setting the \code{packages} parameter to \code{"delta"} in \code{spark_connect()}
#'
#' @family Spark stream serialization
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "local", version = "2.4", packages = "delta")
#'
#' dir.create("text-in")
#' writeLines("A text entry", "text-in/text.txt")
#'
#' text_path <- file.path("file://", getwd(), "text-in")
#'
#' stream <- stream_read_text(sc, text_path) %>% stream_write_delta(path = "delta-test")
#'
#' stream_stop(stream)
#'
#' }
#'
#' @export
stream_write_delta <- function(x,
                               path,
                               mode = c("append", "complete", "update"),
                               checkpoint = file.path("checkpoints", random_string("")),
                               options = list(),
                               ...)
{
  spark_require_version(spark_connection(x), "2.0.0", "Spark streaming")

  sc <- spark_connection(x)

  stream_write_generic(x,
                       path = path,
                       type = "delta",
                       mode = mode,
                       trigger = FALSE,
                       checkpoint = checkpoint,
                       stream_options = options)
}
