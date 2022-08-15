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
      sdf_collect(n = n)

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
    invoke("withWatermark", column, threshold) %>%
    sdf_register()
}

to_milliseconds <- function(dur) {
  if (is.numeric(dur)) {
    dur
  } else if (is.character(dur)) {
    m <- regmatches(dur, regexec("^([0-9]+)\\s*([[:alpha:]]+)$", dur))[[1]]
    conversions <- list(
      ms = 1L,
      msec = 1L,
      msecs = 1L,
      millisecond = 1L,
      milliseconds = 1L,
      s = 1000L,
      sec = 1000L,
      secs = 1000L,
      second = 1000L,
      seconds = 1000L,
      m = 60000L,
      min = 60000L,
      mins = 60000L,
      minute = 60000L,
      minutes = 60000L,
      h = 3600000L,
      hr = 3600000L,
      hrs = 3600000L,
      hour = 3600000L,
      hours = 3600000L,
      d = 86400000L,
      day = 86400000L,
      days = 86400000L
    )
    if (length(m) == 0 || !(m[[3]] %in% names(conversions))) {
      stop(dur, " is not a valid time duration specification")
    }

    as.integer(m[[2]]) * conversions[[m[[3]]]]
  } else {
    stop(
      "time duration specification must be an integer (representing number of ",
      "milliseconds) or a valid time duration string (e.g., '5s')"
    )
  }
}

#' Apply lag function to columns of a Spark Streaming DataFrame
#'
#' Given a streaming Spark dataframe as input, this function will return another
#' streaming dataframe that contains all columns in the input and column(s) that
#' are shifted behind by the offset(s) specified in `...` (see example)
#'
#' @param x An object coercable to a Spark Streaming DataFrame.
#' @param cols A list of expressions for a single or multiple variables to create
#' that will contain the value of a previous entry.
#' @param thresholds Optional named list of timestamp column(s) and
#'   corresponding time duration(s) for deterimining whether a previous record
#'   is sufficiently recent relative to the current record.
#'   If the any of the time difference(s) between the current and a previous
#'   record is greater than the maximal duration allowed, then the previous
#'   record is discarded and will not be part of the query result.
#'   The durations can be specified with numeric types (which will be
#'   interpreted as max difference allowed in number of milliseconds between 2
#'   UNIX timestamps) or time duration strings such as "5s", "5sec", "5min",
#'   "5hour", etc.
#'   Any timestamp column in `x` that is not of timestamp of date Spark SQL
#'   types will be interepreted as number of milliseconds since the UNIX epoch.
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#'
#' sc <- spark_connect(master = "local", version = "2.2.0")
#'
#' streaming_path <- tempfile("days_df_")
#' days_df <- tibble::tibble(
#'   today = weekdays(as.Date(seq(7), origin = "1970-01-01"))
#' )
#' num_iters <- 7
#' stream_generate_test(
#'   df = days_df,
#'   path = streaming_path,
#'   distribution = rep(nrow(days_df), num_iters),
#'   iterations = num_iters
#' )
#'
#' stream_read_csv(sc, streaming_path) %>%
#'   stream_lag(cols = c(yesterday = today ~ 1, two_days_ago = today ~ 2)) %>%
#'   collect() %>%
#'   print(n = 10L)
#' }
#'
#' @export
stream_lag <- function(x, cols, thresholds = NULL) {
  sc <- spark_connection(x)

  spark_require_version(sc, "2.0.0")

  if (!sdf_is_streaming(x)) {
    stop("expected a streaming dataframe as input")
  }

  exprs <- rlang::exprs(!!!cols)
  src_cols <- NULL
  offsets <- NULL
  dst_cols <- NULL
  for (i in seq_along(exprs)) {
    dst <- names(exprs[i])

    expr <- exprs[[i]]
    if (length(expr) != 3 || as.character(expr[[1]]) != "~" ||
      !is.numeric(expr[[3]])) {
      stop(
        "expected `...` to be a comma-separated list of expressions of form",
        "<destination column> = <source column> ~ <offset>"
      )
    }

    src_cols <- c(src_cols, as.character(expr[[2]]))
    offsets <- c(offsets, as.integer(expr[[3]]))
    dst_cols <- c(dst_cols, dst)
  }

  invoke_static(
    sc,
    "sparklyr.StreamUtils",
    "lag",
    x %>% spark_dataframe(),
    as.list(src_cols),
    as.list(offsets),
    as.list(dst_cols),
    as.list(names(thresholds)),
    lapply(unname(thresholds), to_milliseconds)
  ) %>%
    sdf_register()
}
