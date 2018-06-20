stream_read_generic <- function(sc, path, type, schema = NULL)
{
  session <- spark_session(sc)

  if (is.null(schema)) {
    read_obj <- invoke(session, "read")
    read_sdf <- invoke(read_obj, type, path)
    schema <- invoke(read_sdf, "schema")
  }

  invoke(session, "readStream") %>%
    invoke("schema", schema) %>%
    invoke(type, path)
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
  stream_read_generic(sc, path = path, type = "csv")
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
  stream_write_generic(x, path = path, type = "csv")
}
