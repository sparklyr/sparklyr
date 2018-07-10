#' @importFrom jsonlite fromJSON
stream_progress <- function(stream)
{
  invoke(stream, "lastProgress") %>%
    invoke("toString") %>%
    fromJSON()
}

#' View Stream
#'
#' Opens a Shiny gadget to visualize the given stream.
#'
#' @param stream The stream to visualize.
#' @param ... Additional optional arguments.
#'
#' #' @examples
#'
#' library(sparklyr)
#'
#' dir.create("iris-in")
#' write.csv(iris, "iris-in/iris.csv", row.names = FALSE)
#'
#' stream_read_csv("iris-in/") %>%
#'   stream_write_csv("iris-out/") %>%
#'   stream_view() %>%
#'   stream_stop()
#'
#' @import shiny
#' @import r2d3
#' @export
stream_view <- function(
  stream,
  ...
)
{
  validate <- stream_progress(stream)
  interval <- 1000

  ui <- d3Output("plot")
  options <- list(...)

  server <- function(input, output, session) {
    first <- stream_progress(stream)

    output$plot <- renderD3(
      r2d3(
        data = list(
          sources = as.list(first$sources$description),
          sinks = as.list(first$sink$description)
        ),
        script = system.file("streams/stream.js", package = "sparklyr"),
        container = "div",
        options = options
      )
    )

    observe({
      invalidateLater(interval, session)

      data <- stream_progress(stream)

      session$sendCustomMessage(type = "sparklyr_stream_view", list(
        timestamp = data$timestamp,
        rps = list(
          "in" = if (is.numeric(data$inputRowsPerSecond)) floor(data$inputRowsPerSecond) else 0,
          "out" = if (is.numeric(data$processedRowsPerSecond)) floor(data$processedRowsPerSecond) else 0
        )
      ))
    })
  }

  runGadget(ui, server)

  stream
}

#' Render Stream
#'
#' Collects streaming statistics to render the stream as an 'htmlwidget'.
#'
#' @param stream The stream to render
#' @param collect The interval in seconds to collect data before rendering the
#'   'htmlwidget'.
#' @param ... Additional optional arguments.
#'
#' #' @examples
#'
#' library(sparklyr)
#'
#'
#' dir.create("iris-in")
#' write.csv(iris, "iris-in/iris.csv", row.names = FALSE)
#'
#' stream <- stream_read_csv("iris-in/") %>%
#'   stream_write_csv("iris-out/")
#'
#' stream_render(stream)
#' stream_stop(stream)
#'
#' @import r2d3
#' @export
stream_render <- function(
  stream,
  collect = 10,
  ...
)
{
  stats <- list()
  first <- stream_progress(stream)

  for (i in seq_len(collect)) {
    data <- stream_progress(stream)

    message("stream_render: ", data$timestamp, " [", data$inputRowsPerSecond, ":", data$processedRowsPerSecond, "] ", Sys.time())

    stats[[length(stats) + 1]] <- list(
      timestamp = data$timestamp,
      rps = list(
        "in" = if (is.numeric(data$inputRowsPerSecond)) floor(data$inputRowsPerSecond) else 0,
        "out" = if (is.numeric(data$processedRowsPerSecond)) floor(data$processedRowsPerSecond) else 0
      )
    )

    Sys.sleep(1)
  }

  r2d3(
    data = list(
      sources = as.list(first$sources$description),
      sinks = as.list(first$sink$description),
      stats = stats
    ),
    script = system.file("streams/stream.js", package = "sparklyr"),
    container = "div",
    options = options
  )
}
