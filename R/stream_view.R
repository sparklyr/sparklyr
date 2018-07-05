#' View Stream
#'
#' Opens a Shiny gadget to visualize the given stream.
#'
#' @param stream The stream to visualize.
#' @param invalidate The invalidation interval in milliseconds.
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
#' @importFrom jsonlite fromJSON
#' @export
stream_view <- function(
  stream,
  interval = 1000
) {
  ui <- d3Output("plot")

  server <- function(input, output, session) {
    output$plot <- renderD3(
      r2d3(
        data = list(sources = list("FileStreamSource[file]"), sinks = list("FileSink[file]")),
        script = system.file("streams/stream.js", package = "sparklyr")
      )
    )

    observe({
      invalidateLater(interval, session)

      data <- invoke(stream, "lastProgress") %>%
        invoke("toString") %>%
        fromJSON()
    })
  }

  runGadget(ui, server)
}
