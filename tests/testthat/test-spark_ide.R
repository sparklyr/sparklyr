# A fake Spark connection so the RStudio IDE helpers can be exercised without a
# live Spark session. All Spark/DBI calls the helpers make are mocked per-test.
fake_ide_scon <- function(
  master = "local[8]",
  app_name = "sparklyr",
  method = "shell"
) {
  structure(
    list(master = master, app_name = app_name, method = method),
    class = "spark_connection"
  )
}

# ------------------------------ browse_url ------------------------------------
test_that("browse_url does nothing for an empty url", {
  expect_invisible(browse_url(""))
})

test_that("browse_url opens a non-empty url when rstudioapi is unavailable", {
  opened <- NULL
  with_mocked_bindings(
    isAvailable = function(...) FALSE,
    .package = "rstudioapi",
    with_mocked_bindings(
      browseURL = function(url, ...) opened <<- url,
      .package = "utils",
      browse_url("http://localhost:4040")
    )
  )
  expect_equal(opened, "http://localhost:4040")
})

test_that("browse_url translates local urls when rstudioapi is available", {
  opened <- NULL
  with_mocked_bindings(
    isAvailable = function(...) TRUE,
    translateLocalUrl = function(url, absolute = FALSE) {
      paste0("translated:", url)
    },
    .package = "rstudioapi",
    with_mocked_bindings(
      browseURL = function(url, ...) opened <<- url,
      .package = "utils",
      browse_url("http://localhost:4040")
    )
  )
  # a trailing slash is appended before translation
  expect_equal(opened, "translated:http://localhost:4040/")
})

# ------------------------------ host helpers ----------------------------------
test_that("to_host_display collapses a local[n] master to 'local'", {
  expect_equal(to_host_display(fake_ide_scon(master = "local[8]")), "local")
  expect_equal(to_host_display(fake_ide_scon(master = "local[*]")), "local")
  expect_equal(
    to_host_display(fake_ide_scon(master = "spark://host:7077")),
    "spark://host:7077"
  )
})

test_that("to_host combines the host display and the app name", {
  sc <- fake_ide_scon(master = "local[2]", app_name = "myapp")
  expect_equal(to_host(sc), "local - myapp")
})

test_that("external_viewer prefers the observer, falling back to the viewer", {
  obs <- list(id = "observer")
  vw <- list(id = "viewer")

  withr::local_options(connectionObserver = obs, connectionViewer = vw)
  expect_identical(external_viewer(), obs)

  withr::local_options(connectionObserver = NULL)
  expect_identical(external_viewer(), vw)

  withr::local_options(connectionViewer = NULL)
  expect_null(external_viewer())
})

# ----------------------------- DB object helpers ------------------------------
test_that("connection_list_tables returns sorted names, optionally with type", {
  sc <- fake_ide_scon()
  with_mocked_bindings(
    connection_is_open = function(sc) TRUE,
    dbListTables = function(con) c("beta", "alpha"),
    .package = "sparklyr",
    {
      expect_equal(connection_list_tables(sc), c("alpha", "beta"))

      df <- connection_list_tables(sc, includeType = TRUE)
      expect_equal(df$name, c("alpha", "beta"))
      expect_equal(df$type, c("table", "table"))
    }
  )
})

test_that("connection_list_tables handles a null connection", {
  expect_equal(connection_list_tables(NULL), character())

  df <- connection_list_tables(NULL, includeType = TRUE)
  expect_equal(nrow(df), 0L)
  expect_equal(names(df), c("name", "type"))
})

test_that("connection_list_columns describes the columns of a table", {
  sc <- fake_ide_scon()
  with_mocked_bindings(
    connection_is_open = function(sc) TRUE,
    dbGetQuery = function(con, sql) {
      data.frame(a = 1L, b = "x", stringsAsFactors = FALSE)
    },
    .package = "sparklyr",
    {
      cols <- connection_list_columns(sc, table = "t")
      expect_equal(cols$name, c("a", "b"))
      expect_type(cols$type, "character")
    }
  )
})

test_that("connection_list_columns returns NULL for a closed connection", {
  with_mocked_bindings(
    connection_is_open = function(sc) FALSE,
    .package = "sparklyr",
    expect_null(connection_list_columns(fake_ide_scon(), table = "t"))
  )
})

test_that("connection_preview_table runs a limited query", {
  sc <- fake_ide_scon()
  captured <- NULL
  with_mocked_bindings(
    connection_is_open = function(sc) TRUE,
    dbGetQuery = function(con, sql) {
      captured <<- sql
      data.frame(x = 1)
    },
    .package = "sparklyr",
    {
      res <- connection_preview_table(sc, "mytable", 10)
      expect_equal(res, data.frame(x = 1))
      expect_match(captured, "SELECT \\* FROM mytable LIMIT 10")
    }
  )
})

test_that("connection_preview_table returns NULL for a closed connection", {
  with_mocked_bindings(
    connection_is_open = function(sc) FALSE,
    .package = "sparklyr",
    expect_null(connection_preview_table(fake_ide_scon(), "t", 5))
  )
})

test_that("spark_ide_objects/columns/preview dispatch to the DB helpers", {
  sc <- fake_ide_scon()
  with_mocked_bindings(
    connection_is_open = function(sc) TRUE,
    dbListTables = function(con) "tbl",
    dbGetQuery = function(con, sql) data.frame(a = 1L),
    .package = "sparklyr",
    {
      objs <- spark_ide_objects(
        sc,
        catalog = NULL,
        schema = NULL,
        name = NULL,
        type = NULL
      )
      expect_equal(objs$name, "tbl")

      cols <- spark_ide_columns(sc, table = "tbl")
      expect_equal(cols$name, "a")

      prev <- spark_ide_preview(sc, rowLimit = 5, table = "tbl")
      expect_equal(prev, data.frame(a = 1L))
    }
  )
})

# --------------------------- connection lifecycle -----------------------------
test_that("spark_ide_connection_closed notifies the external viewer", {
  closed <- NULL
  obs <- list(
    connectionClosed = function(type, host) {
      closed <<- list(type = type, host = host)
    }
  )
  withr::local_options(connectionObserver = obs, connectionViewer = NULL)

  spark_ide_connection_closed(fake_ide_scon(
    master = "local[1]",
    app_name = "a"
  ))
  expect_equal(closed$type, "Spark")
  expect_equal(closed$host, "local - a")
})

test_that("spark_ide_connection_closed is a no-op without a viewer", {
  withr::local_options(connectionObserver = NULL, connectionViewer = NULL)
  expect_null(spark_ide_connection_closed(fake_ide_scon()))
})

test_that("spark_ide_connection_updated notifies the viewer for normal tables", {
  updated <- NULL
  obs <- list(
    connectionUpdated = function(type, host, hint) updated <<- hint
  )
  withr::local_options(connectionObserver = obs, connectionViewer = NULL)

  spark_ide_connection_updated(fake_ide_scon(), hint = "mytable")
  expect_equal(updated, "mytable")
})

test_that("spark_ide_connection_updated skips sparklyr temp tables", {
  updated <- NULL
  obs <- list(
    connectionUpdated = function(type, host, hint) updated <<- hint
  )
  withr::local_options(connectionObserver = obs, connectionViewer = NULL)

  spark_ide_connection_updated(fake_ide_scon(), hint = "sparklyr_tmp_123")
  expect_null(updated)
})

# ------------------------------- action buttons -------------------------------
test_that("spark_ide_actions builds actions for a yarn shell connection", {
  sc <- fake_ide_scon(method = "shell")
  edited <- NULL
  browsed <- character()
  with_mocked_bindings(
    spark_web = function(sc, ...) "http://web",
    spark_connection_is_yarn = function(sc) TRUE,
    spark_connection_yarn_ui = function(sc) "http://yarn",
    spark_log = function(sc, ...) c("line1", "line2"),
    spark_log_file = function(sc) "spark.log",
    browse_url = function(url) browsed <<- c(browsed, url),
    # `file.edit` is imported via `import(utils)`, so it lives in the sparklyr
    # namespace and must be mocked there (not in "utils").
    file.edit = function(...) edited <<- ..1,
    .package = "sparklyr",
    {
      actions <- spark_ide_connection_actions(sc)
      expect_setequal(names(actions), c("Spark", "YARN", "Log", "Help"))

      # invoke the callbacks so their bodies are covered too
      actions$Spark$callback()
      actions$YARN$callback()
      actions$Log$callback()
      expect_equal(edited, "spark.log")
      expect_true("http://web" %in% browsed)
      expect_true("http://yarn" %in% browsed)
    }
  )
})

test_that("spark_ide_actions builds livy actions without a web url", {
  sc <- fake_ide_scon(method = "livy")
  sc$master <- "http://livy"
  sc$sessionId <- "42"
  visited <- character()
  with_mocked_bindings(
    spark_web = function(sc, ...) "",
    spark_connection_is_yarn = function(sc) FALSE,
    spark_log = function(sc, ...) "line1",
    .package = "sparklyr",
    {
      with_mocked_bindings(
        browseURL = function(url, ...) visited <<- c(visited, url),
        .package = "utils",
        {
          actions <- spark_ide_connection_actions(sc)
          expect_false("Spark" %in% names(actions))
          expect_setequal(names(actions), c("Livy", "Log", "Help"))

          actions$Livy$callback()
          actions$Log$callback()
          actions$Help$callback()
          expect_true(any(grepl("/ui$", visited)))
          expect_true(any(grepl("/log$", visited)))
        }
      )
    }
  )
})

test_that("spark_ide_actions SQL action uses a global `sc` and previews a table", {
  sc <- fake_ide_scon(method = "shell")
  opened <- NULL

  # simulate the RStudio API helper being available in the search path; the
  # callback captures it via get(".rs.api.documentNew")
  assign(
    ".rs.api.documentNew",
    function(type, contents, ...) opened <<- contents,
    envir = globalenv()
  )
  withr::defer(rm(".rs.api.documentNew", envir = globalenv()))

  with_mocked_bindings(
    spark_web = function(sc, ...) "",
    spark_connection_is_yarn = function(sc) FALSE,
    spark_log = function(sc, ...) "x",
    dbListTables = function(con) "fruits",
    .package = "sparklyr",
    {
      actions <- spark_ide_actions(sc)
      expect_true("SQL" %in% names(actions))

      # a global `sc` identical to the connection -> used directly as the name
      assign("sc", sc, envir = globalenv())
      withr::defer(rm("sc", envir = globalenv()))

      actions$SQL$callback()
      expect_match(opened, "-- !preview conn=sc")
      expect_match(opened, "SELECT \\* FROM `fruits`")
    }
  )
})

test_that("spark_ide_actions SQL action resolves the connection variable name", {
  sc <- fake_ide_scon(method = "shell")
  opened <- NULL
  assign(
    ".rs.api.documentNew",
    function(type, contents, ...) opened <<- contents,
    envir = globalenv()
  )
  withr::defer(rm(".rs.api.documentNew", envir = globalenv()))

  with_mocked_bindings(
    spark_web = function(sc, ...) "",
    spark_connection_is_yarn = function(sc) FALSE,
    spark_log = function(sc, ...) "x",
    dbListTables = function(con) character(),
    .package = "sparklyr",
    {
      sql <- spark_ide_actions(sc)$SQL$callback

      # no global `sc`, but another variable holds the connection
      assign("my_spark", sc, envir = globalenv())
      sql()
      expect_match(opened, "-- !preview conn=my_spark")
      expect_match(opened, "SELECT 1") # no tables -> fallback query
      rm("my_spark", envir = globalenv())

      # connection not held by any global variable -> empty name
      sql()
      expect_match(opened, "-- !preview conn=")
    }
  )
})

# ----------------- connection registration (live Spark) -----------------------
# `on_connection_opened()` hands a bundle of callbacks to the RStudio IDE. We
# register a capturing fake observer/viewer, then invoke those callbacks
# ourselves against a real Spark connection to confirm the wiring works.
test_that("on_connection_opened registers v1.1 observer callbacks (live)", {
  sc <- testthat_spark_connection()
  testthat_tbl("mtcars")

  captured <- NULL
  withr::local_options(
    connectionObserver = list(connectionOpened = function(...) {
      captured <<- list(...)
    }),
    connectionViewer = NULL
  )

  spark_ide_connection_open(
    sc,
    env = globalenv(),
    connect_call = "spark_connect(master = 'local')"
  )

  expect_equal(captured$type, "Spark")
  expect_type(captured$listObjectTypes(), "list")

  objs <- captured$listObjects(catalog = NULL, schema = NULL)
  expect_true("mtcars" %in% objs$name)

  cols <- captured$listColumns(table = "mtcars")
  expect_true("mpg" %in% cols$name)

  expect_equal(nrow(captured$previewObject(rowLimit = 5, table = "mtcars")), 5L)

  # invoke disconnect with spark_disconnect mocked so the shared session survives
  disconnected <- FALSE
  with_mocked_bindings(
    spark_disconnect = function(sc, ...) disconnected <<- TRUE,
    .package = "sparklyr",
    captured$disconnect()
  )
  expect_true(disconnected)
})

test_that("on_connection_opened registers a v1.0 viewer finder (live)", {
  sc <- testthat_spark_connection()

  captured <- NULL
  withr::local_options(
    connectionObserver = NULL,
    connectionViewer = list(connectionOpened = function(...) {
      captured <<- list(...)
    })
  )

  spark_ide_connection_open(
    sc,
    env = globalenv(),
    connect_call = "spark_connect()"
  )

  expect_equal(captured$type, "Spark")

  # finder locates the variable holding a matching, open connection
  e <- new.env()
  assign("my_sc", sc, envir = e)
  assign("not_a_conn", 42, envir = e)
  expect_equal(captured$finder(e, to_host(sc)), "my_sc")

  # no matching connection in scope -> NULL
  empty <- new.env()
  assign("x", 1, envir = empty)
  expect_null(captured$finder(empty, "no-such-host"))
})
