skip_connection("core_gateway")
skip_on_livy()
skip_on_arrow_devel()

test_that("master_is_gateway recognizes gateway master URLs", {
  expect_true(master_is_gateway("sparklyr://10.0.0.1:8880"))
  expect_true(master_is_gateway("10.0.0.1:8880/3"))
  expect_false(master_is_gateway("local"))
  expect_false(master_is_gateway("yarn"))
})

test_that("spark_gateway_commands exposes the protocol command codes", {
  cmds <- spark_gateway_commands()
  expect_equal(cmds[["GetPorts"]], 0)
  expect_equal(cmds[["RegisterInstance"]], 1)
})

test_that("gateway_connection rejects a non-gateway master", {
  expect_error(
    gateway_connection("local", list()),
    "expected to be formatted as sparklyr://"
  )
})

test_that("wait_connect_gateway returns NULL when the socket never connects", {
  with_mocked_bindings(
    socketConnection = function(...) stop("connection refused"),
    Sys.sleep = function(...) invisible(NULL),
    .package = "base",
    expect_null(
      wait_connect_gateway(
        "localhost", 9999,
        list(sparklyr.gateway.timeout = 0.05),
        isStarting = FALSE
      )
    )
  )
})

test_that("spark_connect_gateway returns NULL or aborts when the gateway is down", {
  with_mocked_bindings(
    wait_connect_gateway = function(...) NULL,
    .package = "sparklyr",
    {
      expect_null(
        spark_connect_gateway("localhost", 9999, 1, list(), isStarting = FALSE)
      )
      expect_error(
        spark_connect_gateway("localhost", 9999, 1, list(), isStarting = TRUE),
        "did not respond"
      )
    }
  )
})

test_that("spark_gateway_connection aborts when the backend socket fails", {
  gw <- list(gateway = textConnection("x"), backendPort = 9999)
  # the error handler closes gw$gateway; guard the fixture cleanup either way
  withr::defer(tryCatch(close(gw$gateway), error = function(e) NULL))
  with_mocked_bindings(
    socketConnection = function(...) stop("refused"),
    .package = "base",
    expect_error(
      spark_gateway_connection("sparklyr://h:8880", list(), gw, "h"),
      "Failed to open connection to backend"
    )
  )
})

test_that("spark_log / spark_web are unavailable over a gateway connection", {
  sc <- structure(
    list(),
    class = c("spark_gateway_connection", "spark_connection")
  )
  expect_error(spark_log(sc), "not available")
  expect_error(spark_web(sc), "not available")
})

test_clear_cache()
