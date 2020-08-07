stream_class <- function(stream) {
  class(stream) <- c("spark_stream", class(stream))
  stream
}

stream_status <- function(stream) {
  invoke(stream, "status") %>%
    invoke("json") %>%
    jsonlite::fromJSON()
}

#' @export
print.spark_stream <- function(x, ...) {
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

  invisible(x)
}

#' Stops a Spark Stream
#'
#' Stops processing data from a Spark stream.
#'
#' @param stream The spark stream object to be stopped.
#'
#' @export
stream_stop <- function(stream) {
  if (!is.null(stream$job)) rstudio_jobs_api()$add_job_progress(stream$job, 100L)

  stopped <- invoke(stream, "stop")

  stream_unregister(stream)

  invisible(stopped)
}

stream_validate <- function(stream) {
  waitSeconds <- cast_scalar_integer(spark_config_value(config, "sparklyr.stream.validate.timeout", 3)) %>%
    as.difftime(units = "secs")

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

    if (identical(cause, NULL)) {
      cause <- invoke(exception, "get") %>%
        invoke("cause") %>%
        invoke("toString")
    }

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
                                    interval = 1000) {
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
                                      checkpoint = 5000) {
  structure(class = c("stream_trigger_continuous"), list(
    interval = checkpoint
  ))
}

stream_trigger_create <- function(trigger, sc) {
  UseMethod("stream_trigger_create")
}

stream_trigger_create.stream_trigger_interval <- function(trigger, sc) {
  invoke_static(sc, "org.apache.spark.sql.streaming.Trigger", "ProcessingTime", as.integer(trigger$interval))
}

stream_trigger_create.stream_trigger_continuous <- function(trigger, sc) {
  invoke_static(sc, "org.apache.spark.sql.streaming.Trigger", "Continuous", as.integer(trigger$interval))
}

#' Spark Stream's Name
#'
#' Retrieves the name of the Spark stream if available.
#'
#' @param stream The spark stream object.
#'
#' @export
stream_name <- function(stream) {
  invoke(stream, "name")
}

#' Spark Stream's Identifier
#'
#' Retrieves the identifier of the Spark stream.
#'
#' @param stream The spark stream object.
#'
#' @export
stream_id <- function(stream) {
  invoke(stream, "id") %>% invoke("toString")
}

sdf_collect_stream <- function(x, ...) {
  sc <- spark_connection(x)
  args <- list(...)
  n <- args$n

  memory <- stream_write_memory(
    x,
    trigger = stream_trigger_interval(interval = 0)
  )

  data <- data.frame()

  waitSeconds <- cast_scalar_integer(spark_config_value(config, "sparklyr.stream.collect.timeout", 5))

  commandStart <- Sys.time()
  while (nrow(data) == 0 && commandStart + waitSeconds > Sys.time()) {
    data <- tbl(sc, stream_name(memory))

    data <- data %>%
      spark_dataframe() %>%
      sdf_collect()

    Sys.sleep(0.1)
  }

  stream_stop(memory)

  data
}

stream_generate_test_entry <- function(
                                       df,
                                       path,
                                       distribution,
                                       iterations,
                                       interval,
                                       idxDataStart = 1,
                                       idxDataEnd = 1,
                                       idxDist = 1,
                                       idxFile = 1) {
  later <- get("later", envir = asNamespace("later"))

  if (idxFile > iterations) {
    return()
  }

  if (idxDist > length(distribution)) idxDist <- 1

  idxDataStart <- idxDataEnd %% nrow(df)
  idxDataEnd <- idxDataStart + distribution[idxDist]

  dfCycle <- df
  while (nrow(dfCycle) < idxDataEnd) {
    dfCycle <- rbind(dfCycle, df)
  }

  rows <- dfCycle[idxDataStart:idxDataEnd, ]

  write.csv(rows, file.path(path, paste0("stream_", idxFile, ".csv")), row.names = FALSE)
  Sys.sleep(interval)

  idxDist <- idxDist + 1
  idxFile <- idxFile + 1

  later(function() {
    stream_generate_test_entry(
      df,
      path,
      distribution,
      iterations,
      interval,
      idxDataStart,
      idxDataEnd,
      idxDist,
      idxFile
    )
  }, interval)
}

#' Generate Test Stream
#'
#' Generates a local test stream, useful when testing streams locally.
#'
#' @param df The data frame used as a source of rows to the stream, will
#'   be cast to data frame if needed. Defaults to a sequence of one thousand
#'   entries.
#' @param path Path to save stream of files to, defaults to \code{"source"}.
#' @param distribution The distribution of rows to use over each iteration,
#'   defaults to a binomial distribution. The stream will cycle through the
#'   distribution if needed.
#' @param iterations Number of iterations to execute before stopping, defaults
#'   to fifty.
#' @param interval The inverval in seconds use to write the stream, defaults
#'   to one second.
#'
#' @details This function requires the \code{callr} package to be installed.
#'
#' @importFrom stats runif
#' @export
stream_generate_test <- function(
                                 df = rep(1:1000),
                                 path = "source",
                                 distribution = floor(10 + 1e5 * stats::dbinom(1:20, 20, 0.5)),
                                 iterations = 50,
                                 interval = 1) {
  if (!"later" %in% installed.packages()) {
    stop("'stream_generate_test()' requires the 'later' package.")
  }

  if (!is.data.frame(df)) df <- as.data.frame(df)
  if (!dir.exists(path)) dir.create(path, recursive = TRUE)

  stream_generate_test_entry(df, path, distribution, iterations, interval)
}

#' Find Stream
#'
#' Finds and returns a stream based on the stream's identifier.
#'
#' @param sc The associated Spark connection.
#' @param id The stream identifier to find.
#'
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' sdf_len(sc, 10) %>%
#'   spark_write_parquet(path = "parquet-in")
#'
#' stream <- stream_read_parquet(sc, "parquet-in") %>%
#'   stream_write_parquet("parquet-out")
#'
#' stream_id <- stream_id(stream)
#' stream_find(sc, stream_id)
#' }
#'
#' @export
stream_find <- function(sc, id) {
  spark_session(sc) %>%
    invoke("streams") %>%
    invoke("get", id) %>%
    stream_class()
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
stream_watermark <- function(x, column = "timestamp", threshold = "10 minutes") {
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
