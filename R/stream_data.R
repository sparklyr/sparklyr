stream_read_generic_type <- function(sc,
                                     path,
                                     type,
                                     name,
                                     columns = NULL,
                                     header = TRUE,
                                     infer_schema = TRUE,
                                     delimiter = ",",
                                     quote = "\"",
                                     escape = "\\",
                                     charset = "UTF-8",
                                     null_value = NULL,
                                     options = list())
{
  switch(type,
    csv = {
      options <- spark_csv_options(header, infer_schema, delimiter, quote, escape, charset, null_value, options)
      spark_csv_read(
        sc,
        spark_normalize_path(path),
        options,
        columns)
    },
    default = {
      spark_session(sc) %>%
        invoke("read") %>%
        invoke(type, path)
    }
  )
}

stream_read_generic <- function(sc, path, type, name, schema = NULL)
{
  if (is.null(schema)) {
    reader <- stream_read_generic_type(sc, path, type, name)
    schema <- invoke(reader, "schema")
  }

  spark_session(sc) %>%
    invoke("readStream") %>%
    invoke("schema", schema) %>%
    invoke(type, path) %>%
    invoke("createOrReplaceTempView", name)

  tbl(sc, name)
}

stream_write_generic <- function(x, path, type)
{
  sdf <- spark_dataframe(x)
  if (!invoke(sdf, "isStreaming"))
    stop("DataFrame requires streaming context. Use `stream_read_*()` to read from streams.")

  invoke(sdf, "writeStream") %>%
    invoke("format", type) %>%
    invoke("option", "checkpointLocation", file.path(path, "checkpoint")) %>%
    invoke("option", "path", path) %>%
    invoke("start")
}

#' Read a CSV stream into a Spark DataFrame
#'
#' Read a tabular data stream into a Spark DataFrame.
#'
#' @param sc A \code{spark_connection}.
#' @param name The name to assign to the newly generated table.
#' @param path The path to the file. Needs to be accessible from the cluster.
#'   Supports the \samp{"hdfs://"}, \samp{"s3a://"} and \samp{"file://"} protocols.
#' @param ... Optional arguments; currently unused.
#'
#' @family Spark stream serialization
#'
#' @export
stream_read_csv <- function(sc, name, path)
{
  spark_require_version(sc, "2.0.0")
  stream_read_generic(sc, path = path, type = "csv", name = name)
}

#' Write a Spark DataFrame to a CSV stream
#'
#' Write a Spark DataFrame to a tabular (typically, comma-separated) stream
#'
#' @inheritParams stream_read_csv
#' @param x A Spark DataFrame or dplyr operation
#' @param ... Optional arguments; currently unused.
#'
#' @family Spark stream serialization
#'
#' @export
stream_write_csv <- function(x, path)
{
  spark_require_version(spark_connection(x), "2.0.0")
  stream_write_generic(x, path = path, type = "csv")
}
