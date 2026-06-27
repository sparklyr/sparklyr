# ---------------------------------------------------------------------------
# Local tests for the streaming Shiny/r2d3 UI helpers. No live Spark or browser:
# the Spark invocations are mocked and the Shiny server logic is driven with
# shiny::testServer (runGadget is mocked so nothing actually launches).
# ---------------------------------------------------------------------------

skip_if_not_installed("shiny")
skip_if_not_installed("r2d3")

fake_progress <- function() {
  list(
    sources = list(description = "source-1"),
    sink = list(description = "sink-1"),
    timestamp = "2024-01-01T00:00:00.000Z",
    inputRowsPerSecond = 5.7,
    processedRowsPerSecond = 3.2
  )
}

test_that("stream_package_check() errors only for missing packages", {
  expect_silent(stream_package_check("shiny"))
  expect_error(
    stream_package_check("a_package_that_does_not_exist_123"),
    "package is required"
  )
})

test_that("stream_progress() returns NULL or parsed JSON", {
  # lastProgress is NULL -> NULL
  with_mocked_bindings(
    invoke = function(jobj, method, ...) NULL,
    .package = "sparklyr",
    expect_null(stream_progress("stream"))
  )

  # lastProgress present -> toString parsed from JSON
  with_mocked_bindings(
    invoke = function(jobj, method, ...) {
      if (method == "lastProgress") {
        "lp"
      } else {
        '{"timestamp":"t","inputRowsPerSecond":5}'
      }
    },
    .package = "sparklyr",
    {
      progress <- stream_progress("stream")
      expect_equal(progress$timestamp, "t")
      expect_equal(progress$inputRowsPerSecond, 5)
    }
  )
})

test_that("stream_stats() seeds and accumulates per-interval stats", {
  with_mocked_bindings(
    stream_progress = function(stream) fake_progress(),
    .package = "sparklyr",
    {
      stats <- stream_stats("stream")
      expect_equal(stats$sources, "source-1")
      expect_equal(stats$sink, "sink-1")
      expect_length(stats$stats, 1)
      # floored numeric rates
      expect_equal(stats$stats[[1]]$rps[["in"]], 5)
      expect_equal(stats$stats[[1]]$rps[["out"]], 3)

      # a second collection appends to the existing stats
      stats <- stream_stats("stream", stats)
      expect_length(stats$stats, 2)
    }
  )

  # non-numeric rates fall back to 0
  with_mocked_bindings(
    stream_progress = function(stream) {
      p <- fake_progress()
      p$inputRowsPerSecond <- NULL
      p$processedRowsPerSecond <- NULL
      p
    },
    .package = "sparklyr",
    {
      stats <- stream_stats("stream")
      expect_equal(stats$stats[[1]]$rps[["in"]], 0)
      expect_equal(stats$stats[[1]]$rps[["out"]], 0)
    }
  )
})

test_that("stream_render() collects over an interval then builds an r2d3 widget", {
  rendered <- NULL
  with_mocked_bindings(
    stream_stats = function(stream, stats = list()) {
      list(sources = "s", sink = "k", stats = list(list(timestamp = "t")))
    },
    .package = "sparklyr",
    with_mocked_bindings(
      Sys.sleep = function(...) invisible(NULL),
      .package = "base",
      with_mocked_bindings(
        r2d3 = function(data, ...) {
          rendered <<- data
          structure(list(), class = "r2d3_fake")
        },
        .package = "r2d3",
        {
          # collect branch (stats = NULL): loops `collect` times
          widget <- stream_render(stream = "x", collect = 2)
          expect_s3_class(widget, "r2d3_fake")
          expect_equal(rendered$sources, list("s"))

          # stats supplied: skips the collection loop
          widget2 <- stream_render(
            stats = list(
              sources = "s",
              sink = "k",
              stats = list()
            )
          )
          expect_s3_class(widget2, "r2d3_fake")
        }
      )
    )
  )
})

test_that("stream_view() builds the UI and a server that pushes progress", {
  captured <- new.env()

  with_mocked_bindings(
    stream_progress = function(stream) fake_progress(),
    .package = "sparklyr",
    with_mocked_bindings(
      runGadget = function(ui, server) {
        captured$ui <- ui
        captured$server <- server
        invisible(NULL)
      },
      .package = "shiny",
      {
        result <- stream_view("fake-stream")
        expect_identical(result, "fake-stream")
      }
    )
  )

  expect_false(is.null(captured$ui))
  expect_true(is.function(captured$server))

  # drive the captured server to exercise the render + observe logic
  messages <- list()
  with_mocked_bindings(
    stream_progress = function(stream) fake_progress(),
    .package = "sparklyr",
    shiny::testServer(captured$server, {
      session$flushReact()
      expect_false(is.null(output$plot))
    })
  )
})

test_that("reactiveSpark() requires shiny and polls a stream when present", {
  # shiny not installed -> stop
  with_mocked_bindings(
    installed.packages = function(...) matrix("not-shiny"),
    .package = "sparklyr",
    expect_error(reactiveSpark("x"), "shiny")
  )

  # full path: every Spark call mocked, polled through testServer
  fake_df <- data.frame(value = 1L)
  with_mocked_bindings(
    spark_connection = function(x) "sc",
    spark_dataframe = function(x) "sdf",
    invoke = function(jobj, method, ...) jobj,
    invoke_static = function(...) "expr",
    stream_write_memory = function(x, name, ...) "stream",
    stream_stop = function(stream) invisible(NULL),
    spark_session = function(sc) "session",
    sdf_collect = function(x, ...) fake_df,
    random_string = function(prefix) paste0(prefix, "tbl"),
    .package = "sparklyr",
    {
      server <- function(input, output, session) {
        # explicit session (non-NULL branch)
        rs <- reactiveSpark("x", intervalMillis = 5, session = session)
        output$tbl <- shiny::renderTable(rs())
        # session = NULL -> falls back to getDefaultReactiveDomain()
        rs_default <- reactiveSpark("x", intervalMillis = 5)
        output$tbl2 <- shiny::renderTable(rs_default())
      }
      shiny::testServer(server, {
        session$flushReact()
        expect_false(is.null(output$tbl))
        expect_false(is.null(output$tbl2))
      })
    }
  )
})
