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
  active <- invoke(stream, "isActive")

  cat(
    paste(
      paste("Stream:", id),
      paste("Status:", status$message),
      paste("Active: ", active),
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

stream_validate <- function(stream)
{
  exception <- invoke(stream, "exception")
  if (!invoke(exception, "isEmpty")) {
    cause <- invoke(exception, "get") %>%
      invoke("cause") %>%
      invoke("getMessage")
    stop(cause)
  }

  stream
}

#' Spark Stream Interval Trigger
#'
#' Creates a Spark structured streaming trigger to execute
#' over the specified interval.
#'
#' @param interval The execution interval specified in milliseconds.
#'
#' @seealso \code{\link{stream_trigger_continuous}}
#' @export
stream_trigger_interval <- function(
  interval = 5000
)
{
  structure(class = c("stream_trigger_interval"), list(
    interval = interval
  ))
}

#' Spark Stream Continuous Trigger
#'
#' Creates a Spark structured streaming trigger to execute
#' continuously. This mode is the most performant but not all operations
#' are supported.
#'
#' @param checkpoint The checkpoint interval specified in milliseconds.
#'
#' @seealso \code{\link{stream_trigger_interval}}
#' @export
stream_trigger_continuous <- function(
  checkpoint = 5000
)
{
  structure(class = c("stream_trigger_continuous"), list(
    interval = checkpoint
  ))
}

stream_trigger_create <- function(trigger, sc)
{
  UseMethod("stream_trigger_create")
}

stream_trigger_create.stream_trigger_interval <- function(trigger, sc)
{
  invoke_static(sc, "org.apache.spark.sql.streaming.Trigger", "ProcessingTime", as.integer(trigger$interval))
}

stream_trigger_create.stream_trigger_continuous <- function(trigger, sc)
{
  invoke_static(sc, "org.apache.spark.sql.streaming.Trigger", "Continuous", as.integer(trigger$interval))
}
