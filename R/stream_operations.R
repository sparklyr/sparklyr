stream_class <- function(stream)
{
  class(stream) <- c("spark_stream", class(stream))
  stream
}

stream_status <- function(stream)
{
  invoke(stream, "status") %>%
    invoke("json") %>%
    jsonlite::fromJSON()
}

#' @export
print.spark_stream <- function(x, ...)
{
  id <- stream_id(x)
  status <- stream_status(x)
  active <- invoke(x, "isActive")

  cat(
    paste(
      paste("Stream:", id),
      paste("Status:", status$message),
      paste("Active:", active),
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
  if (!is.null(stream$job)) rstudio_jobs_api()$add_job_progress(stream$job, 100L)

  stopped <- invoke(stream, "stop")

  if (!is.null(stream$job)) rstudio_jobs_api()$remove_job(stream$job)

  invisible(stopped)
}

stream_validate <- function(stream)
{
  waitSeconds <- ensure_scalar_integer(spark_config_value(config, "sparklyr.stream.validate.timeout", 3))

  commandStart <- Sys.time()
  while (!grepl("waiting", stream_status(stream)$message) &&
         commandStart + waitSeconds > Sys.time()) {
    Sys.sleep(0.1)
  }

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
  interval = 1000
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

#' Spark Stream's Name
#'
#' Retrieves the name of the Spark stream if available.
#'
#' @param stream The spark stream object.
#'
#' @export
stream_name <- function(stream)
{
  invoke(stream, "name")
}

#' Spark Stream's Identifier
#'
#' Retrieves the identifier of the Spark stream.
#'
#' @param stream The spark stream object.
#'
#' @export
stream_id <- function(stream)
{
  invoke(stream, "id") %>% invoke("toString")
}

sdf_collect_stream <- function(x, ...)
{
  sc <- spark_connection(x)
  args <- list(...)
  n <- args$n

  # collection is mostly used to print to console using tibble, otherwise, reactiveSpark
  # or a proper sink makes more sense.
  if (is.null(n)) n <- getOption("dplyr.print_min", getOption("tibble.print_min", 10))

  memory <- stream_write_memory(
    x,
    trigger = stream_trigger_interval(interval = 0)
  )

  data <- data.frame()

  waitSeconds <- ensure_scalar_integer(spark_config_value(config, "sparklyr.stream.collect.timeout", 5))

  commandStart <- Sys.time()
  while (nrow(data) == 0 && commandStart + waitSeconds > Sys.time()) {
    data <- tbl(sc, stream_name(memory))

    if (!identical(n, Inf)) data <- data %>% head(n)

    data <- data %>%
      spark_dataframe() %>%
      sdf_collect()

    Sys.sleep(0.1)
  }

  stream_stop(memory)

  data
}

#' @importFrom stats runif
stream_generate_random <- function(df, path = "streams-random", interval = 5)
{
  if (!"later" %in% installed.packages()) {
    stop("'stream_generate_random()' requires the 'later' package.")
  }

  if (!dir.exists(path)) dir.create(path)

  later <- get("later", envir = asNamespace("later"))

  min <- floor(runif(1, 1, length(df)))
  max <- floor(runif(1, min, length(df)))

  write.csv(df[min:max, ], paste(file.path(path, random_string("rand_")), "csv", sep = "."), row.names = FALSE)

  later(function() {
    stream_generate_random(df, path, interval)
  }, interval)
}

#' Find Stream
#'
#' Finds and returns a stream based on the stream's itentifier.
#'
#' @param sc The associated Spark connection.
#' @param id The stream identifier to find.
#'
#' @export
stream_find <- function(sc, id)
{
  spark_session(sc) %>% invoke("streams") %>% invoke("get", id) %>% stream_class()
}

#' Watermark Stream
#'
#' Ensures a stream has a watermark defined, which is required for some
#' operations over streams.
#'
#' @param x An object coercable to a Spark Streaming DataFrame.
#' @param column The name of the column that contains the event time of the row,
#'   if the column is missing, a column with the current time will be added.
#' @param threshold The minimum delay to wait to data to arrive late, defaults
#'   to ten minutes.
#'
#' @export
stream_watermark <- function(x, column = "timestamp", threshold = "10 minutes")
{
  sdf <- spark_dataframe(x)
  sc <- spark_connection(x)

  if (!column %in% invoke(sdf, "columns")) {
    sdf <- sdf %>%
      invoke(
        "withColumn",
        column,
        invoke_static(sc, "org.apache.spark.sql.functions", "expr", "current_timestamp()")
      )
  }

  sdf %>%
    invoke("withWatermark", "timestamp", threshold) %>%
    sdf_register()
}
