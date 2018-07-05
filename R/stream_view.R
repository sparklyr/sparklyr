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
#' @export
stream_view <- function(
  stream,
  interval = 1000
) {
  ui <- d3Output("plot")

  server <- function(input, output, session) {
    output$plot <- renderD3(
      r2d3(
        data = c (0.3, 0.6, 0.8, 0.95, 0.40, 0.20),
        script = system.file("examples/barchart.js", package = "r2d3")
      )
    )

    observe({
      invalidateLater(interval, session)
    })
  }

  runGadget(ui, server)
}
