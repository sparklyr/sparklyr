stream_class <- function(stream)
{
  class(stream) <- c("spark_stream", class(stream))
  stream
}

#' @export
print.spark_stream <- function(stream)
{
  id <- invoke(invoke(stream, "id"), "toString")
  status <- invoke(stream, "status") %>%
    invoke("json") %>%
    jsonlite::fromJSON()

  cat(
    paste(
      paste("Stream:", id),
      paste("Status:", status$message),
      paste("Data Available:", status$isDataAvailable),
      paste("Trigger Active:", status$isTriggerActive),
      "",
      sep = "\n"
    )
  )
}

#' Stops a Spark Stream
#'
#' Stops processing data from a Spark stream.
#'
#' @param stream The spark stream object to be stopped.
#'
#' @export
stream_stop <- function(stream)
{
  invoke(stream, "stop") %>%
    invisible()
}
