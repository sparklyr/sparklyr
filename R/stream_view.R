#' @importFrom jsonlite fromJSON
stream_progress <- function(stream)
{
  lastProgress <- invoke(stream, "lastProgress")

  if (is.null(lastProgress)) {
    NULL
  }
  else {
    lastProgress %>%
      invoke("toString") %>%
      fromJSON()
  }
}

#' View Stream
#'
#' Opens a Shiny gadget to visualize the given stream.
#'
#' @param stream The stream to visualize.
#' @param ... Additional optional arguments.
#'
#' @examples
#' \dontrun{
#' library(sparklyr)
#' sc <- spark_connect(master = "local")
#'
#' dir.create("iris-in")
#' write.csv(iris, "iris-in/iris.csv", row.names = FALSE)
#'
#' stream_read_csv(sc, "iris-in/") %>%
#'   stream_write_csv("iris-out/") %>%
#'   stream_view() %>%
#'   stream_stop()
#' }
#' @import r2d3
#' @export
stream_view <- function(
  stream,
  ...
)
{
  if (!"shiny" %in% installed.packages()) stop("The 'shiny' package is required for this operation.")

  validate <- stream_progress(stream)
  interval <- 1000

  shinyUI <- get("shinyUI", envir = asNamespace("shiny"))
  tags <- get("tags", envir = asNamespace("shiny"))
  div <- get("div", envir = asNamespace("shiny"))
  HTML <- get("HTML", envir = asNamespace("shiny"))
  ui <- shinyUI(
    div(
      tags$head(
        tags$style(HTML("
          html, body, body > div {
            width: 100%;
            height: 100%;
            margin: 0px;
          }
        "))
      ),
      d3Output("plot", width = "100%", height = "100%")
    )
  )

  options <- list(...)

  observe <- get("observe", envir = asNamespace("shiny"))
  invalidateLater <- get("invalidateLater", envir = asNamespace("shiny"))
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

  runGadget <- get("runGadget", envir = asNamespace("shiny"))
  runGadget(ui, server)

  stream
}

#' Stream Statistics
#'
#' Collects streaming statistics, usually, to be used with \code{stream_render()}
#' to render streaming statistics.
#'
#' @param stream The stream to collect statistics from.
#' @param stats An optional stats object generated using \code{stream_stats()}.
#'
#' @return A stats object containing streaming statistics that can be passed
#'   back to the \code{stats} parameter to continue aggregating streaming stats.
#'
#' @examples
#'\dontrun{
#' sc <- spark_connect(master = "local")
#' sdf_len(sc, 10) %>%
#'   spark_write_parquet(path = "parquet-in")
#'
#' stream <- stream_read_parquet(sc, "parquet-in") %>%
#'  stream_write_parquet("parquet-out")
#'
#' stream_stats(stream)
#' }
#'
#' @export
stream_stats <- function(stream, stats = list()) {
  data <- stream_progress(stream)

  if (is.null(stats$stats)) {
    stats$sources <- data$sources$description
    stats$sink <- data$sink$description
    stats$stats <- list()
  }

  stats$stats[[length(stats$stats) + 1]] <- list(
    timestamp = data$timestamp,
    rps = list(
      "in" = if (is.numeric(data$inputRowsPerSecond)) floor(data$inputRowsPerSecond) else 0,
      "out" = if (is.numeric(data$processedRowsPerSecond)) floor(data$processedRowsPerSecond) else 0
    )
  )

  stats
}

#' Render Stream
#'
#' Collects streaming statistics to render the stream as an 'htmlwidget'.
#'
#' @param stream The stream to render
#' @param collect The interval in seconds to collect data before rendering the
#'   'htmlwidget'.
#' @param stats Optional stream statistics collected using \code{stream_stats()},
#'   when specified, \code{stream} should be omitted.
#' @param ... Additional optional arguments.
#'
#' @examples
#' \dontrun{
#' library(sparklyr)
#' sc <- spark_connect(master = "local")
#'
#' dir.create("iris-in")
#' write.csv(iris, "iris-in/iris.csv", row.names = FALSE)
#'
#' stream <- stream_read_csv(sc, "iris-in/") %>%
#'   stream_write_csv("iris-out/")
#'
#' stream_render(stream)
#' stream_stop(stream)
#' }
#' @import r2d3
#' @export
stream_render <- function(
  stream = NULL,
  collect = 10,
  stats = NULL,
  ...
)
{
  if (is.null(stats)) {
    stats <- stream_stats(stream)

    for (i in seq_len(collect)) {
      Sys.sleep(1)
      stats <- stream_stats(stream, stats)
    }
  }

  r2d3(
    data = list(
      sources = as.list(stats$sources),
      sinks = as.list(stats$sink),
      stats = stats$stats
    ),
    script = system.file("streams/stream.js", package = "sparklyr"),
    container = "div",
    options = options
  )
}
