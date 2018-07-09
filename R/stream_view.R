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
#' @param invalidate The invalidation interval in milliseconds.
#' @param ... Additional optional arguments.
#'
#' #' @examples
#'
#' library(sparklyr)
#'
#' stream_read_csv() %>%
#'   stream_write_csv() %>%
#'   stream_view(stream) %>%
#'   stream_stop()
#'
#' @import shiny
#' @import r2d3
#' @export
stream_view <- function(
  stream,
  interval = 1000,
  ...
) {
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
          "in" = floor(data$inputRowsPerSecond),
          "out" = floor(data$processedRowsPerSecond)
        )
      ))
    })
  }

  runGadget(ui, server)

  stream
}
